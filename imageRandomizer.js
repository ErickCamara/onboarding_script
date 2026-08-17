/**
 * Gera variações de uma imagem com hash (MD5) diferente a cada execução,
 * mas visualmente indistinguíveis a olho nu. Usado pelas etapas de adição
 * de imagem do onboarding para nunca reenviar a mesma imagem duas vezes.
 *
 * Para brokers que precisam de uma URL pública (Infobip), o resultado
 * também é subido no S3 (bucket nefex-imagens-publicas, key UUID), com a
 * bucket policy de leitura pública garantida antes do primeiro upload.
 *
 * Variáveis de ambiente opcionais:
 *   S3_BUCKET (default nefex-imagens-publicas)
 *   AWS_REGION (default us-east-1)
 *   SKIP_PUBLIC_POLICY_SETUP=1  -> pula a configuração automática de acesso público
 *
 * IMPORTANTE: para a configuração automática de acesso público funcionar, a
 * identidade IAM usada precisa, além de s3:PutObject, também de:
 *   - s3:GetBucketPolicy
 *   - s3:PutBucketPolicy
 *   - s3:GetBucketPublicAccessBlock
 *   - s3:PutBucketPublicAccessBlock
 * Se a conta tiver "Block Public Access" habilitado a nível de CONTA (via
 * AWS Organizations / S3 Control), isso precisa ser ajustado separadamente
 * por um admin — não é algo que a PutPublicAccessBlockCommand no nível do
 * bucket consiga sobrepor.
 */

require('dotenv').config();
const { createHash, randomBytes, randomUUID } = require('crypto');
const sharp = require('sharp');
const {
  GetBucketPolicyCommand,
  PutBucketPolicyCommand,
  PutObjectCommand,
  PutPublicAccessBlockCommand,
  S3Client,
} = require('@aws-sdk/client-s3');

const S3_BUCKET = process.env.S3_BUCKET || 'nefex-imagens-publicas';
const AWS_REGION = process.env.AWS_REGION || process.env.AWS_SQS_REGION || 'us-east-1';
const SKIP_PUBLIC_POLICY_SETUP = process.env.SKIP_PUBLIC_POLICY_SETUP === '1';

const NOISE_STDDEV = 1.3;
const GAMMA_MIN = 0.99;
const GAMMA_MAX = 1.01;
const SCALE_MIN = 0.999;
const SCALE_MAX = 0.993; // encolhe 0.1–0.7%

function randomBetween(min, max) {
  return min + Math.random() * (max - min);
}

function gaussianRandom(stddev) {
  let u = 0;
  let v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v) * stddev;
}

function pickShiftEdge() {
  const edges = ['top', 'bottom', 'left', 'right'];
  return edges[Math.floor(Math.random() * edges.length)];
}

function applyGaussianNoise(data, width, height, channels) {
  for (let i = 0; i < width * height * channels; i++) {
    const noisy = data[i] + gaussianRandom(NOISE_STDDEV);
    data[i] = Math.max(0, Math.min(255, Math.round(noisy)));
  }
}

function applyGammaPerChannel(data, width, height, channels, gamma) {
  if (channels < 3) return;

  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const idx = (y * width + x) * channels;
      data[idx] = Math.max(0, Math.min(255, Math.round(data[idx] * gamma.r)));
      data[idx + 1] = Math.max(0, Math.min(255, Math.round(data[idx + 1] * gamma.g)));
      data[idx + 2] = Math.max(0, Math.min(255, Math.round(data[idx + 2] * gamma.b)));
    }
  }
}

function applyBorderShift(data, width, height, channels, edge) {
  const out = Buffer.from(data);

  switch (edge) {
    case 'top':
      for (let x = 0; x < width; x++) {
        const src = x * channels;
        const dst = (width + x) * channels;
        for (let c = 0; c < channels; c++) {
          out[dst + c] = data[src + c];
        }
      }
      break;
    case 'bottom':
      for (let x = 0; x < width; x++) {
        const src = ((height - 1) * width + x) * channels;
        const dst = ((height - 2) * width + x) * channels;
        for (let c = 0; c < channels; c++) {
          out[dst + c] = data[src + c];
        }
      }
      break;
    case 'left':
      for (let y = 0; y < height; y++) {
        const src = y * width * channels;
        const dst = (y * width + 1) * channels;
        for (let c = 0; c < channels; c++) {
          out[dst + c] = data[src + c];
        }
      }
      break;
    case 'right':
      for (let y = 0; y < height; y++) {
        const src = (y * width + width - 1) * channels;
        const dst = (y * width + width - 2) * channels;
        for (let c = 0; c < channels; c++) {
          out[dst + c] = data[src + c];
        }
      }
      break;
  }

  return out;
}

function insertJpegCommentMarker(jpegBuffer) {
  const soiIndex = jpegBuffer.indexOf(Buffer.from([0xff, 0xd8]));
  if (soiIndex === -1) {
    throw new Error('Buffer JPEG inválido: SOI não encontrado.');
  }

  const commentPayloadSize = 4 + Math.floor(Math.random() * 28);
  const commentPayload = randomBytes(commentPayloadSize);
  const segmentLength = commentPayload.length + 2;
  const segment = Buffer.alloc(2 + segmentLength);
  segment[0] = 0xff;
  segment[1] = 0xfe;
  segment.writeUInt16BE(segmentLength, 2);
  commentPayload.copy(segment, 4);

  const insertAt = soiIndex + 2;
  const result = Buffer.concat([
    jpegBuffer.subarray(0, insertAt),
    segment,
    jpegBuffer.subarray(insertAt),
  ]);

  return { buffer: result, commentBytes: commentPayload.length };
}

function computeMd5(buffer) {
  return createHash('md5').update(buffer).digest('hex');
}

function buildPublicUrl(bucket, region, key) {
  return `https://${bucket}.s3.${region}.amazonaws.com/${key}`;
}

/**
 * Gera UMA variação da imagem de entrada (buffer JPEG), com hash MD5
 * diferente a cada chamada mas visualmente indistinguível.
 */
async function randomizeOnce(inputBuffer, options = {}) {
  const qualityMin = options.qualityMin ?? 85;
  const qualityMax = options.qualityMax ?? 99;
  const quality = Math.round(randomBetween(qualityMin, qualityMax));

  const gamma = {
    r: randomBetween(GAMMA_MIN, GAMMA_MAX),
    g: randomBetween(GAMMA_MIN, GAMMA_MAX),
    b: randomBetween(GAMMA_MIN, GAMMA_MAX),
  };
  const shift = pickShiftEdge();
  const scale = randomBetween(SCALE_MAX, SCALE_MIN);

  const image = sharp(inputBuffer);
  const metadata = await image.metadata();
  const width = metadata.width;
  const height = metadata.height;

  if (width == null || height == null) {
    throw new Error('Não foi possível ler dimensões da imagem de entrada.');
  }

  const { data, info } = await image
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });

  const channels = info.channels;
  const pixelData = Buffer.from(data);

  applyGaussianNoise(pixelData, width, height, channels);
  applyGammaPerChannel(pixelData, width, height, channels, gamma);
  const shifted = applyBorderShift(pixelData, width, height, channels, shift);

  const scaledWidth = Math.max(1, Math.round(width * scale));
  const scaledHeight = Math.max(1, Math.round(height * scale));

  const jpegBuffer = await sharp(shifted, {
    raw: { width, height, channels },
  })
    .resize(scaledWidth, scaledHeight)
    .resize(width, height)
    .jpeg({ quality, mozjpeg: true })
    .toBuffer();

  const { buffer: withComment, commentBytes } = insertJpegCommentMarker(jpegBuffer);

  const params = { quality, gamma, shift, scale, commentBytes };

  return {
    buffer: withComment,
    md5: computeMd5(withComment),
    params,
  };
}

/**
 * Garante que o bucket tenha uma bucket policy liberando leitura pública
 * (s3:GetObject) para todos os objetos, e desbloqueia as flags de
 * "Block Public Access" a nível de bucket que impediriam essa policy de
 * ter efeito. Idempotente: se a policy já concede leitura pública, não
 * faz nada.
 */
async function ensurePublicReadPolicy(s3, bucket) {
  const bucketArn = `arn:aws:s3:::${bucket}`;
  const publicReadStatement = {
    Sid: 'PublicReadGetObject',
    Effect: 'Allow',
    Principal: '*',
    Action: 's3:GetObject',
    Resource: `${bucketArn}/*`,
  };

  let currentPolicy = { Version: '2012-10-17', Statement: [] };

  try {
    const existing = await s3.send(new GetBucketPolicyCommand({ Bucket: bucket }));
    if (existing.Policy) {
      currentPolicy = JSON.parse(existing.Policy);
    }
  } catch (err) {
    if (err?.name !== 'NoSuchBucketPolicy') {
      throw err;
    }
    // Sem policy ainda — segue com o objeto default (Statement vazio).
  }

  const alreadyPublic = currentPolicy.Statement?.some(
    (s) =>
      s.Effect === 'Allow' &&
      s.Action === 's3:GetObject' &&
      (s.Principal === '*' || s.Principal?.AWS === '*') &&
      (s.Resource === `${bucketArn}/*` ||
        (Array.isArray(s.Resource) && s.Resource.includes(`${bucketArn}/*`)))
  );

  if (alreadyPublic) {
    console.log({ event: 'public_policy_already_set', bucket });
    return;
  }

  // Precisa desbloquear "Block Public Access" a nível de bucket antes de a
  // policy pública ter efeito, senão o PutBucketPolicy é aceito mas ignorado
  // (ou rejeitado, dependendo da configuração).
  await s3.send(
    new PutPublicAccessBlockCommand({
      Bucket: bucket,
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        IgnorePublicAcls: true,
        BlockPublicPolicy: false,
        RestrictPublicBuckets: false,
      },
    })
  );

  const newPolicy = {
    Version: '2012-10-17',
    Statement: [...currentPolicy.Statement, publicReadStatement],
  };

  await s3.send(
    new PutBucketPolicyCommand({
      Bucket: bucket,
      Policy: JSON.stringify(newPolicy),
    })
  );

  console.log({ event: 'public_policy_applied', bucket });
}

async function uploadToS3(s3, buffer, key) {
  await s3.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      Body: buffer,
      ContentType: 'image/jpeg',
    })
  );

  return buildPublicUrl(S3_BUCKET, AWS_REGION, key);
}

let s3ClientSingleton = null;
function getS3Client() {
  if (!s3ClientSingleton) {
    s3ClientSingleton = new S3Client({ region: AWS_REGION });
  }
  return s3ClientSingleton;
}

let publicPolicyEnsured = false;
async function ensurePublicPolicyOnce() {
  if (SKIP_PUBLIC_POLICY_SETUP || publicPolicyEnsured) return;
  await ensurePublicReadPolicy(getS3Client(), S3_BUCKET);
  publicPolicyEnsured = true;
}

/**
 * Gera uma variação única da imagem e sobe pro S3, retornando a URL pública
 * — usado quando o broker precisa de uma URL (ex: Infobip).
 */
async function randomizeAndUploadImage(inputBuffer, options = {}) {
  const result = await randomizeOnce(inputBuffer, options);
  await ensurePublicPolicyOnce();
  const s3Key = `${randomUUID()}.jpeg`;
  const publicUrl = await uploadToS3(getS3Client(), result.buffer, s3Key);
  return { ...result, s3Key, publicUrl };
}

module.exports = {
  randomizeOnce,
  randomizeAndUploadImage,
};
