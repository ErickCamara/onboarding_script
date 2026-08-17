require('dotenv').config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { waMetaDatabase, lakeDatabase, wa360Database, waInfoBipDatabase, waMetaWorkAroundDatabase, wa360WorkAroundDatabase, waInfoBipWorkAroundDatabase, waMetaTemplateConfigPoolDatabase, wa360TemplateConfigPoolDatabase, waInfoBipTemplateConfigPoolDatabase } = require('./database/whatsappDb.js')
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { randomizeOnce, randomizeAndUploadImage } = require('./imageRandomizer.js');

// ==========================================================================
// CREDENCIAIS (vêm do .env, nunca ficam hardcoded no script)
// ==========================================================================
const NEFEX_API_TOKEN = process.env.NEFEX_API_TOKEN || '';
const NEFEX_AUTH_HEADER = `Bearer ${NEFEX_API_TOKEN}`;
const TEST_PHONE_NUMBER = process.env.TEST_PHONE_NUMBER || '558387778320';

const sqsClient = new SQSClient({
  region: process.env.AWS_SQS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_SQS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SQS_SECRET_ACCESS_KEY
  }
});

const dbClient = new DynamoDBClient({
  region: process.env.AWS_DYNAMO_REGION,
  credentials: {
    accessKeyId: process.env.AWS_DYNAMO_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_DYNAMO_SECRET_ACCESS_KEY
  }
});

const QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/906212135557/queueMetaWhatsappOfficialReceiveMessageChannels.fifo";
const infoBipVar = '[{"type": "QUICK_REPLY", "parameter": "Iniciar_Atendimento"}, {"type": "QUICK_REPLY", "parameter": "Denunciar"}]'

async function sendToSQS(integrationId, urlFile) {
  const message = {
    id: crypto.randomUUID(),
    to: TEST_PHONE_NUMBER,
    body: urlFile,
    text: urlFile,
    type: "text/plain",
    agent: "API",
    isFake: false,
    poolId: "a1609c42-f5e3-4a24-8ff1-1ae8f72c10a0",
    caption: urlFile,
    isGroup: false,
    idClient: "0ea4ea08-9d94-4fd0-9648-9ea5c737786a",
    isMassive: true,
    externalId: crypto.randomUUID(),
    integrationId: integrationId,
  };

  const params = {
    QueueUrl: QUEUE_URL,
    MessageBody: JSON.stringify(message),
    MessageGroupId: integrationId,
    MessageDeduplicationId: message.id
  };

  try {
    const response = await sqsClient.send(new SendMessageCommand(params));
    console.log("Mensagem enviada com sucesso para SQS:", response.MessageId);
    return response;
  } catch (error) {
    console.error("Erro ao enviar mensagem para SQS:", error);
  }
}

async function processIntegrationData(filePath) {
  try {
    const data = await fs.promises.readFile(path.resolve(filePath), 'utf8');
    const lines = data.split('\n');
    const integrations = lines.map(line => {
      const parts = line.split(', ');
      return {
        number: parts[0],
        externalId: parts[1],
        templateName: parts[7],
        integrationId: parts[6],
        token: parts[8],
        type: parts[4],
        lang: parts[9],
        hasParam: parts[10] ? parts[10].trim().toLowerCase() : "yes"
      };
    });
    console.log('##### integrations: ', integrations);
    return integrations;
  } catch (error) {
    console.error('Error reading or processing the file:', error);
    return [];
  }
}

const tableName = "whatsapp.service.provider.config";

const docClient = DynamoDBDocumentClient.from(dbClient);

async function writeToDynamoDB(id, token) {
  const params = {
    TableName: tableName,
    Item: {
      id: id,
      externalConfig: {
        token: token
      }
    }
  };

  try {
    const data = await docClient.send(new PutCommand(params));
    console.log('Item inserido com sucesso:', data);
  } catch (error) {
    console.error('Erro ao inserir item:', error);
  }
}

const integrationsCreated = [];

let addTemplateConfig = false;
const wantsToWriteToDynamo = true;
const wantToTest = true;

const TEST_BODY_TYPES = {
  BUTTON_URL_VAR: 'BUTTON_URL_VAR',
  INFOBIP_2_BUTTONS: 'INFOBIP_2_BUTTONS',
  BODY_VAR: 'BODY_VAR',
  BODY_NO_VAR: 'BODY_NO_VAR',
};

const testBodyType = TEST_BODY_TYPES.BODY_VAR;

function buildComponents(bodyType) {
  switch (bodyType) {
    case TEST_BODY_TYPES.BUTTON_URL_VAR:
      return [
        { type: "body", parameters: [{ type: "text", text: "TESTE_WEBHOOK" }] },
        { type: "button", subType: "url", index: "0", parameters: [{ type: "text", text: "KrgcnL8" }] }
      ];
    case TEST_BODY_TYPES.INFOBIP_2_BUTTONS:
      return [
        { type: "body", parameters: [{ type: "text", text: "TESTE_WEBHOOK" }] },
        { type: "button", sub_type: "quick_reply", index: "0", parameters: [{ type: "text", text: "Iniciar_Atendimento" }] },
        { type: "button", sub_type: "quick_reply", index: "0", parameters: [{ type: "text", text: "Denunciar" }] }
      ];
    case TEST_BODY_TYPES.BODY_VAR:
      return [{ type: "body", parameters: [{ type: "text", text: "TESTE_WEBHOOK" }] }];
    case TEST_BODY_TYPES.BODY_NO_VAR:
      return [{ type: "body" }];
    default:
      throw new Error(`Tipo de teste de body desconhecido: ${bodyType}`);
  }
}

async function createIntegration(bodyType = testBodyType) {
  const integrations = await processIntegrationData('./dados.txt');

  for (let integration of integrations) {
    const existingId = await checkIfExists(integration.number);
    console.log('existingId: ', existingId);
    const method = existingId ? "put" : "post";
    const url = existingId ? `https://application.nefex.io/api/v1/integrations/whatsapp/${existingId}` : `https://application.nefex.io/api/v1/integrations/whatsapp/`;
    console.log('url: ', url);

    try {
      const response = await axios.request({
        method: method,
        url: url,
        data: {
          name: `Nefex Oficial ${integration.number}`,
          accountName: integration.number,
          accountDescription: `${integration.number}`,
          description: integration.number,
          group: false,
          state: "ACTIVE",
          broker: integration.type === '360' ? "_360DIALOG" : integration.type,
          billingType: "SESSION",
          creditLimit: 10000000,
          trial: false,
          blocking: false,
          official: true,
          moorseManagement: false,
          webhooks: null,
          externalId: integration.externalId,
          step: 'LIVE',
          clientId: "0a661044-bec4-48f9-a58c-1acb780e8eaf"
        },
        headers: {
          Authorization: NEFEX_AUTH_HEADER,
        }
      });

      console.log('RESPONSE: ', response.data.errors);
      integration.integrationId = response.data.data.id;

      if (integration.type === '360') {
        console.log(`Integração ${integration.number} é do tipo 360 — pulando criação e indo direto para teste...`);
        if (addTemplateConfig) {
          try {
            await wa360Database({
              integrationId: response.data.data.id,
              name: integration.templateName || 'Default Template',
              metaParams: integration.metaParams ? integration.metaParams : []
            });
            console.log("Template inserido na base");
          } catch (templateError) {
            console.error('Falha ao inserir template (360), seguindo mesmo assim:', templateError.message);
          }
        }
      } else if (integration.type === 'META') {
        if (addTemplateConfig) {
          const language = integration.lang || null;
          try {
            await waMetaDatabase({
              integrationId: response.data.data.id,
              name: integration.templateName || 'Default Template',
              metaParams: integration.metaParams ? integration.metaParams : [],
              language: language
            });
            console.log("Template inserido na base");
          } catch (templateError) {
            console.error('Falha ao inserir template (META), seguindo mesmo assim:', templateError.message);
          }
        }
      } else if (integration.type === 'INFOBIP') {
        console.log(`Integração ${integration.number} é do tipo INFOBIP — pulando criação e indo direto para teste...`);
        if (addTemplateConfig) {
          const language = integration.lang || null;
          try {
            await waInfoBipDatabase({
              integrationId: response.data.data.id,
              name: integration.templateName || 'Default Template',
              metaParams: integration.metaParams ? integration.metaParams : [],
              language: language
            });
            console.log("Template inserido na base");
          } catch (templateError) {
            console.error('Falha ao inserir template (INFOBIP), seguindo mesmo assim:', templateError.message);
          }
        }
      }

      if (wantsToWriteToDynamo) {
        await writeToDynamoDB(response.data.data.id, integration.token).then(() => {
          console.log("Integrações processadas e dados inseridos no DynamoDB");
        });
      }

      let wasDelivered = ""
      if (wantToTest) {
        if (integration.lang === 'en') {
          wasDelivered = await sendToSQS(response.data.data.id, "TESTE_WEBHOOK_EN");
        } else if (integration.lang === 'pt') {
          wasDelivered = await sendToSQS(response.data.data.id, "TESTE_WEBHOOK_PT");
        } else {
          wasDelivered = await sendMessage(integration.integrationId, integration.templateName, integration.hasParam, bodyType)
        }
      }

      const dataToSave = {
        id: response.data.data.id,
        accountName: response.data.data.accountName,
        token: integration.token,
        templateName: integration.templateName || 'Default Template',
        metaParams: integration.metaParams ? integration.metaParams : [],
        wasDelivered: wasDelivered,
      };
      integrationsCreated.push(dataToSave);

      fs.writeFileSync("integracoes-criadas.json", JSON.stringify(integrationsCreated, null, 2));
    } catch (error) {
      fs.writeFileSync("integracoes-jampa.json", JSON.stringify(integrationsCreated));
      console.error(error?.response?.data || error.message || error);
    }
  }

  return integrationsCreated;
}

async function checkIfExists(number) {
  const url = `https://application.nefex.io/api/v1/integrations/whatsapp?accountName=${number}&sort=name&size=100&page=0`;
  const headers = {
    accept: 'application/json',
    authorization: NEFEX_AUTH_HEADER
  };

  try {
    const response = await axios.get(url, { headers });
    if (response.data?.data?.content && response.data?.data?.content.length > 0) {
      return response.data.data.content[0].id;
    }
  } catch (error) {
    console.error("Erro ao verificar a existência:", error);
  }
  return null;
}

async function verifyDelivery(integrationId, controlId, maxRetries = 0) {
  console.log('verifying message delivery');
  let retries = 0;

  const url = `https://5a19gtrdl7.execute-api.us-east-1.amazonaws.com/Prod/lake-monitoring/chats/${integrationId}/contacts/${TEST_PHONE_NUMBER}/conversations`;
  const headers = {
    'Authorization': NEFEX_AUTH_HEADER,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Accept': 'application/json'
  };

  while (retries < maxRetries) {
    try {
      const response = await lakeDatabase({ controlId })
      console.log('####### response: ', response);

      const checkDelivered = response.some(message => message.status === "DELIVERED");
      console.log('was delivered? ', checkDelivered);

      if (checkDelivered) {
        console.log('Message delivered successfully!');
        return true;
      }

      const isDelivered = response.some(message => message.status !== "DELIVERED");
      if (isDelivered) {
        console.log('Message status is not DELIVERED. Retrying in 5 seconds...');
        retries++;
        await sleep(5000);
        continue;
      }

      const isError = response.some(message => message.status !== "ERROR");
      if (isError) {
        console.log('Message status is ERROR. The number needs to be verified...');
        return false;
      }

      console.log('Message status is neither DELIVERED nor PENDING. Exiting.');
      return false;

    } catch (error) {
      console.error('Error fetching data:', error);
      return false;
    }
  }

  console.log('Max retries reached. Message not delivered.');
  return false;
}

async function sendMessage(integrationId, templateName, hasParam = "yes", bodyType = testBodyType) {
  try {
    console.log('starting message test');
    console.log('bodyType em uso: ', bodyType);

    const response = await axios.post(
      `https://application.nefex.io/api/v1/channels/whatsapp/${integrationId}/send-template`,
      {
        "components": buildComponents(bodyType),
        "templateName": `${templateName}`,
        "to": TEST_PHONE_NUMBER
      },
      {
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'Authorization': NEFEX_AUTH_HEADER
        }
      }
    );

    console.log('SEND MESSAGE RESPONSE: ');
    const controlId = response?.data?.data?.control
    await sleep(10000);

    return
    const wasDelivered = await verifyDelivery(integrationId, controlId);
    return wasDelivered;
  } catch (error) {
    console.log('Erro ao enviar mensagens: ', error);
  }
}

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

const CREDENCIAIS_FILE_PATH = './credenciais.txt';

const INFOBIP_BASE_URL = 'https://2yjjdw.api-us.infobip.com';
const INFOBIP_API_KEY = process.env.INFOBIP_API_KEY || '';

const INFOBIP_LOGO_OPTIONS = [
  { label: 'Bellinati', url: 'https://nefex-imagens-publicas.s3.us-east-1.amazonaws.com/Bellinati_v08.jpeg' },
  { label: 'Itaú (preta)', url: 'https://nefex-imagens-publicas.s3.us-east-1.amazonaws.com/images/itau_preta.jpeg' },
  { label: 'Santander Varejo', url: 'https://nefex-imagens-publicas.s3.us-east-1.amazonaws.com/images/santander_varejo.png' },
  { label: 'IZA', url: 'https://nefex-imagens-publicas.s3.us-east-1.amazonaws.com/IZA_2.png' },
  { label: 'ML Gomes', url: 'https://nefex-imagens-publicas.s3.us-east-1.amazonaws.com/mlgomes.png' },
];

const IMAGENS_LOCAIS_FILE_PATH = './imagens-locais.txt';

function readImagensLocais(filePath) {
  if (!fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8')
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const idx = line.indexOf(',');
      if (idx === -1) return null;
      const label = line.slice(0, idx).trim();
      const imagePath = line.slice(idx + 1).trim();
      if (!label || !imagePath) return null;
      return { label, path: imagePath };
    })
    .filter(Boolean);
}

function parseCredentialLine(rawLine) {
  const line = rawLine.trim();
  if (!line) return null;

  const parts = line.split(',').map(p => p.trim()).filter(p => p !== '');

  if (parts.length === 1) {
    return { broker: 'INFOBIP', sender: parts[0] };
  }
  if (parts.length === 3 && parts[1].toUpperCase() === '360') {
    return { broker: '360', profileCode: parts[0], token: parts[2] };
  }
  if (parts.length === 4 && parts[1].toUpperCase() === 'META') {
    return { broker: 'META', profileCode: parts[0], token: parts[2], appId: parts[3] };
  }

  console.log(`Linha em formato não reconhecido, ignorando: "${rawLine}"`);
  return null;
}

function readCredenciais(filePath) {
  if (!fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8')
    .split('\n')
    .map(rawLine => {
      const parsed = parseCredentialLine(rawLine);
      return parsed ? { ...parsed, rawLine } : null;
    })
    .filter(Boolean);
}

async function upload360Image(imageBuffer, token) {
  const fileLength = imageBuffer.length;
  const initUrl = `https://waba-v2.360dialog.io/uploads?file_length=${fileLength}&file_type=image/jpeg`;

  const initResp = await axios.post(initUrl, null, {
    headers: { 'Content-Type': 'application/json', 'D360-API-KEY': token }
  });
  const uploadId = initResp.data.id;

  const completeResp = await axios.post(`https://waba-v2.360dialog.io/${uploadId}`, imageBuffer, {
    headers: {
      'D360-API-KEY': token,
      'file_offset': '0',
      'Connection': 'close',
      'Content-Type': 'multipart/form-data',
    }
  });

  return completeResp.data.h;
}

async function send360ProfilePhoto(token, profileCode, handle) {
  const response = await axios.post(
    'https://waba-v2.360dialog.io/whatsapp_business_profile',
    { messaging_product: 'whatsapp', profile_picture_handle: handle },
    { headers: { 'Content-Type': 'application/json', 'D360-API-KEY': token } }
  );
  return response.data;
}

async function uploadMetaImage(imageBuffer, token, appId) {
  const fileLength = imageBuffer.length;
  const initUrl = `https://graph.facebook.com/v19.0/${appId}/uploads?file_length=${fileLength}&file_type=image%2Fjpeg&access_token=${token}`;

  const initResp = await axios.post(initUrl, null, {
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` }
  });
  const uploadId = initResp.data.id;

  const completeResp = await axios.post(`https://graph.facebook.com/v19.0/${uploadId}`, imageBuffer, {
    headers: {
      'Authorization': `OAuth ${token}`,
      'file_offset': '0',
      'Connection': 'close',
      'Content-Type': 'multipart/form-data',
    }
  });

  return completeResp.data.h;
}

async function sendMetaProfilePhoto(token, profileCode, handle) {
  const response = await axios.post(
    `https://graph.facebook.com/v19.0/${profileCode}/whatsapp_business_profile`,
    { messaging_product: 'whatsapp', profile_picture_handle: handle },
    { headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } }
  );
  return response.data;
}

async function sendInfobipProfilePhoto(sender, logoUrl) {
  const response = await axios.patch(
    `${INFOBIP_BASE_URL}/whatsapp/1/senders/${sender}/business-info`,
    { logoUrl },
    {
      headers: {
        Authorization: `App ${INFOBIP_API_KEY}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      }
    }
  );
  return response.data;
}

/**
 * Baixa uma imagem de uma URL pública e retorna como Buffer — usado pra
 * pegar o logo base do Infobip antes de gerar a variação randomizada.
 */
async function downloadImageBuffer(url) {
  const response = await axios.get(url, { responseType: 'arraybuffer' });
  return Buffer.from(response.data);
}

// ==========================================================================
// ONBOARDING COMPLETO (em fases sequenciais, cada uma passando por TODOS
// os números do dados.txt antes de seguir pra próxima)
// Fases: 1) criar/editar todas as integrações (automática) -> 2) escolhe o
// MODO: massivo (mesma config coletada 1x e aplicada a vários números do
// mesmo pool, com resumo pra aprovação e opção de excluir números — bom
// pra "várias integrações pro mesmo pool") ou padrão (número a número).
//
// No modo PADRÃO: estratégia de template — template_config (aplicado a
// todas) OU template_config_pool (número a número, com suporte a múltiplos
// pools por número e opção de pular o resto), mutuamente exclusivos ->
// integration_work_around (por integração) -> adição de imagem (por
// integração) -> teste de envio (valida ANTES de colocar no pool) -> só
// com bodyType BODY_VAR: pergunta se quer adicionar direto ao pool via Nyx
// sem cadastrar template_config_pool (pede confirmação do link do
// encurtador + pool_id) -> pool via Nyx (onlyMassive/onlyInsert forçados
// true quando a integração está em mais de um pool).
//
// No modo MASSIVO (runMassBatchConfig): coleta template/work_around/imagem/
// capacidade do pool via Nyx UMA VEZ, mostra resumo consolidado, deixa
// excluir números, aplica a config nos números incluídos -> teste de envio
// -> só então chama a Nyx com a capacidade já coletada (applyMassNyxPool).
//
// Em ambos os modos, depois: DynamoDB -> tabela resumo (console + JSON).
// Falha numa fase pra uma integração específica não impede as próximas
// fases; só falha na fase 1 tira a integração do resto do fluxo.
// ==========================================================================

/**
 * Cria (POST) ou edita (PUT) a integração na API, dependendo se já existe.
 * Lança erro se a chamada falhar (quem chama decide o que fazer).
 */
async function createOrUpdateIntegration(integration, existingId) {
  const method = existingId ? "put" : "post";
  const url = existingId
    ? `https://application.nefex.io/api/v1/integrations/whatsapp/${existingId}`
    : `https://application.nefex.io/api/v1/integrations/whatsapp/`;

  const response = await axios.request({
    method,
    url,
    data: {
      name: `Nefex Oficial ${integration.number}`,
      accountName: integration.number,
      accountDescription: `${integration.number}`,
      description: integration.number,
      group: false,
      state: "ACTIVE",
      broker: integration.type === '360' ? "_360DIALOG" : integration.type,
      billingType: "SESSION",
      creditLimit: 10000000,
      trial: false,
      blocking: false,
      official: true,
      moorseManagement: false,
      webhooks: null,
      externalId: integration.externalId,
      step: 'LIVE',
      clientId: "0a661044-bec4-48f9-a58c-1acb780e8eaf"
    },
    headers: {
      Authorization: NEFEX_AUTH_HEADER,
    }
  });

  return response.data.data;
}

/**
 * Insere/atualiza a config de template no banco certo, de acordo com o broker.
 */
async function addTemplateToDatabase(integration) {
  const language = integration.lang || null;
  const params = {
    integrationId: integration.integrationId,
    name: integration.templateName || 'Default Template',
    metaParams: integration.metaParams ? integration.metaParams : [],
    language
  };

  if (integration.type === '360') {
    return wa360Database(params);
  } else if (integration.type === 'META') {
    return waMetaDatabase(params);
  } else if (integration.type === 'INFOBIP') {
    return waInfoBipDatabase(params);
  }

  throw new Error(`Tipo de broker desconhecido para template config: ${integration.type}`);
}

/**
 * Pergunta s/n e valida a resposta, repetindo até ser válida.
 */
async function askYesNo(rl, question) {
  while (true) {
    const raw = (await ask(rl, `${question} (s/n): `)).toLowerCase();
    if (['s', 'sim', 'y', 'yes'].includes(raw)) return true;
    if (['n', 'nao', 'não', 'no'].includes(raw)) return false;
    console.log(`Resposta inválida ("${raw}"). Responda com "s" ou "n".`);
  }
}

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Pergunta um valor e valida que é um UUID, repetindo até ser válido.
 * Pega de cara erros de digitação em campos que exigem UUID.
 */
async function askUuid(rl, question) {
  while (true) {
    const raw = (await ask(rl, question)).trim();
    if (UUID_REGEX.test(raw)) return raw;
    console.log(`Valor inválido ("${raw}"). Esperado um UUID (formato xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).`);
  }
}

/**
 * Mostra um resumo dos dados coletados e pede confirmação antes de seguir
 * pra uma chamada irreversível (insert, API externa, upload de imagem etc.).
 */
async function confirmSummary(rl, title, fields) {
  console.log(`\n${title}`);
  Object.entries(fields).forEach(([key, value]) => {
    console.log(`  ${key}: ${value}`);
  });
  return askYesNo(rl, 'Confirma esses dados?');
}

/**
 * Pergunta, para UMA integração, se ela deve ser inserida/atualizada no
 * integration_work_around e com qual valor de block_response.
 * OBS: a origem de "new_from" ainda não foi confirmada, então por enquanto
 * é perguntada aqui manualmente. Assim que a fonte certa for definida, dá
 * pra trocar essa linha para pegar o valor automaticamente (ex: integration.number).
 */
/**
 * Coleta block_response + new_from (com resumo de confirmação), sem
 * perguntar "quer fazer isso?" nem aplicar no banco — usado tanto pelo
 * fluxo padrão (por integração) quanto pelo modo massivo (uma vez só).
 */
async function askWorkAroundSharedFields(rl) {
  while (true) {
    // TODO: confirmar a origem correta do new_from (por enquanto, pergunta manual)
    const blockResponse = await askYesNo(rl, 'A flag block_response deve ser true?');
    const newFrom = await ask(rl, 'Valor do new_from: ');

    const confirmed = await confirmSummary(rl, 'Resumo do integration_work_around:', {
      block_response: blockResponse,
      new_from: newFrom,
    });
    if (confirmed) return { blockResponse, newFrom };
    console.log('Vamos preencher novamente.');
  }
}

/**
 * Insere/atualiza o integration_work_around no banco certo, de acordo com
 * o broker. Não pergunta nada — os valores já vêm prontos.
 */
async function applyWorkAround(integration, { blockResponse, newFrom }) {
  const params = {
    integrationId: integration.integrationId,
    newFrom,
    blockResponse,
  };

  if (integration.type === '360') {
    await wa360WorkAroundDatabase(params);
  } else if (integration.type === 'META') {
    await waMetaWorkAroundDatabase(params);
  } else if (integration.type === 'INFOBIP') {
    await waInfoBipWorkAroundDatabase(params);
  } else {
    console.log(`Tipo de broker desconhecido (${integration.type}), pulando integration_work_around.`);
    return 'skipped';
  }

  return 'applied';
}

async function handleIntegrationWorkAround(rl, integration) {
  const wantsWorkAround = await askYesNo(
    rl,
    `Deseja inserir/atualizar a integração ${integration.number} no integration_work_around?`
  );

  if (!wantsWorkAround) {
    console.log('Pulando integration_work_around para esta integração.');
    return 'skipped';
  }

  const { blockResponse, newFrom } = await askWorkAroundSharedFields(rl);
  return applyWorkAround(integration, { blockResponse, newFrom });
}

/**
 * Insere a config de template na tabela template_config_pool do banco certo,
 * de acordo com o broker. name e integrationId vêm da própria integração
 * (dados.txt / contexto do loop); o restante (newFrom, poolId, buttonUrl,
 * newIntegrationId) vem de poolConfig, coletado previamente do usuário.
 */
async function addTemplateToPoolDatabase(integration, poolConfig) {
  const params = {
    integrationId: integration.integrationId,
    name: integration.templateName || 'Default Template',
    newFrom: poolConfig.newFrom,
    poolId: poolConfig.poolId,
    buttonUrl: poolConfig.buttonUrl,
    newIntegrationId: poolConfig.newIntegrationId,
  };

  if (integration.type === '360') {
    return wa360TemplateConfigPoolDatabase(params);
  } else if (integration.type === 'META') {
    return waMetaTemplateConfigPoolDatabase(params);
  } else if (integration.type === 'INFOBIP') {
    return waInfoBipTemplateConfigPoolDatabase(params);
  }

  throw new Error(`Tipo de broker desconhecido para template_config_pool: ${integration.type}`);
}

/**
 * Pergunta, uma única vez (na 1ª integração do loop), qual configuração de
 * template usar nesta execução do onboarding completo: template_config
 * (tabela padrão) ou template_config_pool. São mutuamente exclusivos —
 * nunca cadastra nas duas tabelas na mesma execução.
 */
async function askTemplateStrategy(rl, { disallowTemplateConfig = false } = {}) {
  while (true) {
    console.log('\nQual configuração de template deseja usar nesta execução?');
    console.log(disallowTemplateConfig
      ? '1 - template_config (indisponível para o bodyType BUTTON_URL_VAR)'
      : '1 - template_config (padrão)');
    console.log('2 - template_config_pool');
    console.log('3 - Nenhuma das duas');

    const raw = await ask(rl, '\nEscolha uma opção: ');
    if (raw === '1') {
      if (disallowTemplateConfig) {
        console.log('template_config não pode ser usado com o bodyType BUTTON_URL_VAR. Escolha 2 ou 3.');
        continue;
      }
      return 'template_config';
    }
    if (raw === '2') return 'template_config_pool';
    if (raw === '3') return 'none';
    console.log(`Opção inválida ("${raw}"). Tente novamente.`);
  }
}

/**
 * Coleta os campos do template_config_pool que não vêm automaticamente do
 * dados.txt/contexto (name e integration_id são resolvidos à parte).
 */
async function askTemplateConfigPoolSharedFields(rl) {
  while (true) {
    const newFrom = await ask(rl, 'Valor do new_from (template_config_pool): ');
    const poolId = await askUuid(rl, 'Valor do pool_id (UUID): ');
    const buttonUrl = await ask(rl, 'Valor do button_url: ');
    const newIntegrationId = await askUuid(rl, 'Valor do new_integration_id (UUID): ');

    const confirmed = await confirmSummary(rl, 'Resumo do template_config_pool:', {
      new_from: newFrom,
      pool_id: poolId,
      button_url: buttonUrl,
      new_integration_id: newIntegrationId,
    });
    if (confirmed) return { newFrom, poolId, buttonUrl, newIntegrationId };
    console.log('Vamos preencher novamente.');
  }
}

const NYX_POOL_INTEGRATIONS_URL = 'https://api-nyx.nefex.io/api/v1/nyx/pools/integrations/create';

/**
 * broker no formato esperado pela API da Nyx: mesmo mapeamento usado na
 * criação da integração (createOrUpdateIntegration).
 */
function resolveBrokerForNyx(type) {
  return type === '360' ? '_360DIALOG' : type;
}

/**
 * Adiciona uma integração ao pool via API da Nyx. poolId vem do que foi
 * coletado na etapa de template_config_pool; tamanhoMensal é sempre 50000.
 */
async function addIntegrationToNyxPool({ integrationId, poolId, broker, tamanhoDiario, onlyMassive, onlyInsert }) {
  const response = await axios.post(
    NYX_POOL_INTEGRATIONS_URL,
    {
      uuids: [integrationId],
      poolId,
      broker,
      tamanhoMensal: 50000,
      tamanhoDiario,
      onlyMassive,
      onlyInsert,
    },
    {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        Authorization: NEFEX_AUTH_HEADER,
      }
    }
  );
  return response.data;
}

/**
 * Pergunta um valor numérico e valida, repetindo até ser válido.
 */
async function askNumber(rl, question) {
  while (true) {
    const raw = await ask(rl, question);
    const parsed = Number(raw);
    if (raw.trim() !== '' && !Number.isNaN(parsed)) return parsed;
    console.log(`Valor inválido ("${raw}"). Digite um número.`);
  }
}

/**
 * Fase 1: checa existência e cria/edita TODAS as integrações do dados.txt,
 * automaticamente (sem perguntas). Só as que tiverem sucesso seguem pras
 * próximas fases — ficam em `ready`, já com integrationId preenchido.
 * Também monta a linha inicial de cada integração na tabela resumo.
 */
async function phaseCreateOrUpdateAll(integrations) {
  console.log('\n=== ETAPA 1: criar/editar integrações ===');
  const ready = [];
  const summary = [];

  for (const integration of integrations) {
    const summaryEntry = {
      number: integration.number,
      integrationId: null,
      criacao: 'erro',
      templateStrategy: '-',
      poolsConfigured: 0,
      workAround: '-',
      imagem: '-',
      dynamo: '-',
      teste: '-',
    };
    summary.push(summaryEntry);

    try {
      const existingId = await checkIfExists(integration.number);
      const integrationData = await createOrUpdateIntegration(integration, existingId);
      integration.integrationId = integrationData.id;
      integration.accountName = integrationData.accountName;
      summaryEntry.integrationId = integrationData.id;
      summaryEntry.criacao = existingId ? 'atualizada' : 'criada';
      ready.push(integration);
      console.log(`✅ ${integration.number}: integração ${existingId ? 'atualizada' : 'criada'} (id: ${integrationData.id}).`);
    } catch (error) {
      console.error(`❌ ${integration.number}: falha ao checar/criar/editar integração.`, error?.response?.data || error.message);
    }
  }

  return { ready, summary };
}

/**
 * Pergunta, pra UM número, se quer configurar template_config_pool agora,
 * pular este número, ou pular todos os restantes de uma vez.
 */
async function askNumberScopeChoice(rl, number, actionLabel = 'Configurar template_config_pool') {
  while (true) {
    console.log(`\nNúmero: ${number}`);
    console.log(`1 - ${actionLabel} para este número`);
    console.log('2 - Pular este número');
    console.log('3 - Pular todos os restantes');

    const raw = await ask(rl, '\nEscolha uma opção: ');
    if (raw === '1') return 'configure';
    if (raw === '2') return 'skip';
    if (raw === '3') return 'skip-rest';
    console.log(`Opção inválida ("${raw}"). Tente novamente.`);
  }
}

/**
 * Sub-fluxo da fase 3 (quando a estratégia é template_config_pool): pergunta
 * número a número se quer configurar, com suporte a MAIS DE UM
 * template_config_pool por número (loop até dizer que não quer mais) e uma
 * opção de pular todos os números restantes de uma vez.
 */
async function collectTemplateConfigPoolForAllNumbers(rl, ready, summary) {
  const poolConfigsCollected = [];

  for (const integration of ready) {
    const choice = await askNumberScopeChoice(rl, integration.number);

    if (choice === 'skip-rest') {
      console.log('Pulando template_config_pool para todos os números restantes.');
      break;
    }
    if (choice === 'skip') {
      console.log(`Pulando template_config_pool para ${integration.number}.`);
      continue;
    }

    let poolsForThisNumber = 0;
    let wantsMore = true;
    while (wantsMore) {
      const poolConfig = await askTemplateConfigPoolSharedFields(rl);

      try {
        await addTemplateToPoolDatabase(integration, poolConfig);
        console.log(`✅ Template cadastrado no template_config_pool para ${integration.number}.`);
        poolConfigsCollected.push({ integration, poolConfig, insertStatus: 'ok' });
        poolsForThisNumber++;
      } catch (error) {
        console.error(`❌ Falha ao cadastrar template_config_pool para ${integration.number}.`, error.message);
      }

      wantsMore = await askYesNo(rl, `Deseja cadastrar mais um template_config_pool para ${integration.number}?`);
    }

    const summaryEntry = summary.find(s => s.number === integration.number);
    if (summaryEntry) summaryEntry.poolsConfigured = poolsForThisNumber;
  }

  return poolConfigsCollected;
}

/**
 * Fluxo rápido (só disponível com bodyType BODY_VAR): adiciona números
 * direto ao pool via Nyx, SEM cadastrar template_config_pool — usado
 * quando o template/pool já existem e só falta vincular os números.
 * Pede confirmação de que o link do encurtador (button_url) já foi
 * ajustado manualmente, e um único pool_id compartilhado pra todos os
 * números escolhidos nesta execução.
 */
async function collectDirectNyxPoolForAllNumbers(rl, ready, summary) {
  const linkAjustado = await askYesNo(
    rl,
    'Você já validou/ajustou o link do encurtador (button_url) para esses números?'
  );
  if (!linkAjustado) {
    console.log('Ajuste o link do encurtador antes de continuar. Pulando adição direta ao pool via Nyx.');
    return [];
  }

  const poolId = await askUuid(rl, 'Valor do pool_id (UUID) a usar para todos os números: ');
  const poolConfigsCollected = [];

  for (const integration of ready) {
    const choice = await askNumberScopeChoice(rl, integration.number, 'Adicionar direto ao pool via Nyx');

    if (choice === 'skip-rest') {
      console.log('Pulando adição direta ao pool via Nyx para todos os números restantes.');
      break;
    }
    if (choice === 'skip') {
      console.log(`Pulando adição direta ao pool via Nyx para ${integration.number}.`);
      continue;
    }

    poolConfigsCollected.push({
      integration,
      poolConfig: { poolId, newFrom: null, buttonUrl: null, newIntegrationId: null },
      insertStatus: 'não aplicável (direto ao Nyx)',
    });

    const summaryEntry = summary.find(s => s.number === integration.number);
    if (summaryEntry) summaryEntry.poolsConfigured = 1;
  }

  return poolConfigsCollected;
}

/**
 * Fase extra (só com bodyType BODY_VAR), rodada DEPOIS do teste de envio:
 * pergunta se quer adicionar as integrações já testadas/validadas direto ao
 * pool via Nyx (sem cadastrar template_config_pool). Fica de propósito
 * depois do teste — primeiro testa e valida, só depois coloca no pool.
 */
async function phaseDirectNyxPool(rl, ready, summary) {
  console.log('\n=== ETAPA 6: adicionar direto ao pool via Nyx (opcional, pós-teste) ===');

  const wantsDirectPool = await askYesNo(
    rl,
    'Deseja adicionar essas integrações direto ao pool via Nyx (sem cadastrar template_config_pool)?'
  );
  if (!wantsDirectPool) {
    console.log('Pulando adição direta ao pool via Nyx.');
    return [];
  }

  return collectDirectNyxPoolForAllNumbers(rl, ready, summary);
}

/**
 * Fase 2: escolhe a estratégia de template (uma vez): template_config ou
 * template_config_pool são mutuamente exclusivos. template_config aplica
 * automaticamente pra todas as integrações; template_config_pool entra no
 * sub-fluxo número a número (collectTemplateConfigPoolForAllNumbers).
 */
async function phaseTemplateStrategy(rl, ready, disallowTemplateConfig, summary) {
  console.log('\n=== ETAPA 2: configuração de template ===');
  const strategy = await askTemplateStrategy(rl, { disallowTemplateConfig });
  let poolConfigsCollected = [];

  if (strategy === 'template_config') {
    for (const integration of ready) {
      const summaryEntry = summary.find(s => s.number === integration.number);
      try {
        await addTemplateToDatabase(integration);
        console.log(`✅ Template inserido/atualizado em template_config para ${integration.number}.`);
        if (summaryEntry) summaryEntry.templateStrategy = 'template_config';
      } catch (error) {
        console.error(`❌ Falha ao inserir/atualizar template_config para ${integration.number}.`, error.message);
        if (summaryEntry) summaryEntry.templateStrategy = 'template_config (erro)';
      }
    }
  } else if (strategy === 'template_config_pool') {
    summary.forEach(s => { s.templateStrategy = 'template_config_pool'; });
    poolConfigsCollected = await collectTemplateConfigPoolForAllNumbers(rl, ready, summary);
  } else {
    summary.forEach(s => { s.templateStrategy = 'nenhuma'; });
    console.log('Nenhuma estratégia de template selecionada — pulando template_config e template_config_pool.');
  }

  return { strategy, poolConfigsCollected };
}

/**
 * Fase 3: pergunta, integração por integração, se quer cadastrar no
 * integration_work_around (comportamento igual ao de hoje).
 */
async function phaseWorkAround(rl, ready, summary) {
  console.log('\n=== ETAPA 3: integration_work_around ===');
  for (const integration of ready) {
    const summaryEntry = summary.find(s => s.number === integration.number);
    try {
      const status = await handleIntegrationWorkAround(rl, integration);
      if (summaryEntry) summaryEntry.workAround = status === 'applied' ? 'sim' : 'não';
    } catch (error) {
      console.error(`❌ Falha na etapa de integration_work_around para ${integration.number}.`, error.message);
      if (summaryEntry) summaryEntry.workAround = 'erro';
    }
  }
}

/**
 * Fase 4: pergunta, integração por integração, se quer atualizar a imagem
 * de perfil (comportamento igual ao de hoje).
 */
async function phaseImage(rl, ready, summary) {
  console.log('\n=== ETAPA 4: adição de imagem ===');
  for (const integration of ready) {
    const summaryEntry = summary.find(s => s.number === integration.number);
    try {
      const status = await handleIntegrationImageUpdate(rl, integration);
      if (summaryEntry) {
        summaryEntry.imagem = status === 'applied' ? 'sim' : (status === 'failed' ? 'erro' : 'não');
      }
    } catch (error) {
      console.error(`❌ Falha na etapa de adição de imagem para ${integration.number}.`, error.message);
      if (summaryEntry) summaryEntry.imagem = 'erro';
    }
  }
}

/**
 * Fase 5: adiciona ao pool via Nyx cada combinação integração+pool coletada
 * na fase 2. Se uma integração está em mais de um pool, onlyMassive e
 * onlyInsert são forçados para true (regra de negócio confirmada com o
 * usuário); com um único pool, os dois são perguntados normalmente.
 * tamanhoDiario é perguntado por pool, já que pode variar entre eles.
 */
async function phaseNyxPool(rl, poolConfigsCollected, summary) {
  console.log('\n=== ETAPA 7: pool via Nyx ===');

  if (!poolConfigsCollected.length) {
    console.log('Nenhuma configuração de template_config_pool coletada, pulando etapa.');
    return;
  }

  const wantsNyx = await askYesNo(rl, 'Deseja adicionar as integrações configuradas aos pools via Nyx?');
  if (!wantsNyx) {
    console.log('Pulando etapa de pool via Nyx a pedido do usuário.');
    return;
  }

  const countByIntegrationId = {};
  poolConfigsCollected.forEach(({ integration }) => {
    countByIntegrationId[integration.integrationId] = (countByIntegrationId[integration.integrationId] || 0) + 1;
  });

  const resolvedFlagsByIntegrationId = {};

  for (const entry of poolConfigsCollected) {
    const { integration, poolConfig } = entry;
    const isMultiPool = countByIntegrationId[integration.integrationId] > 1;

    if (!resolvedFlagsByIntegrationId[integration.integrationId]) {
      if (isMultiPool) {
        console.log(`\nIntegração ${integration.number} está em mais de um pool: onlyMassive e onlyInsert forçados para true.`);
        resolvedFlagsByIntegrationId[integration.integrationId] = { onlyMassive: true, onlyInsert: true };
      } else {
        const onlyMassive = await askYesNo(rl, `onlyMassive deve ser true para ${integration.number}?`);
        const onlyInsert = await askYesNo(rl, `onlyInsert deve ser true para ${integration.number}?`);
        resolvedFlagsByIntegrationId[integration.integrationId] = { onlyMassive, onlyInsert };
      }
    }

    const { onlyMassive, onlyInsert } = resolvedFlagsByIntegrationId[integration.integrationId];

    let tamanhoDiario, confirmed;
    do {
      tamanhoDiario = await askNumber(rl, `Valor do tamanhoDiario (${integration.number} / pool ${poolConfig.poolId}): `);
      confirmed = await confirmSummary(rl, `Resumo do pool via Nyx (${integration.number}):`, {
        pool_id: poolConfig.poolId,
        tamanhoDiario,
        onlyMassive,
        onlyInsert,
      });
    } while (!confirmed);

    entry.nyx = { tamanhoDiario, onlyMassive, onlyInsert, status: 'pendente' };

    try {
      await addIntegrationToNyxPool({
        integrationId: integration.integrationId,
        poolId: poolConfig.poolId,
        broker: resolveBrokerForNyx(integration.type),
        tamanhoDiario,
        onlyMassive,
        onlyInsert,
      });
      console.log(`✅ Integração ${integration.number} adicionada ao pool ${poolConfig.poolId} via Nyx.`);
      entry.nyx.status = 'ok';
    } catch (error) {
      console.error(`❌ Falha ao adicionar ${integration.number} ao pool ${poolConfig.poolId} via Nyx.`, error?.response?.data || error.message);
      entry.nyx.status = 'erro';
    }
  }
}

/**
 * Pergunta, uma única vez logo após criar/editar as integrações, qual modo
 * usar no resto do fluxo: massivo (mesma config pra várias integrações do
 * mesmo pool) ou padrão (número a número, como já existia).
 */
async function askOnboardingMode(rl) {
  while (true) {
    console.log('\nComo você quer configurar essas integrações?');
    console.log('1 - Configuração massiva (mesma configuração pra todas, associadas a 1 pool)');
    console.log('2 - Configuração padrão (número a número)');

    const raw = await ask(rl, '\nEscolha uma opção: ');
    if (raw === '1') return 'mass';
    if (raw === '2') return 'standard';
    console.log(`Opção inválida ("${raw}"). Tente novamente.`);
  }
}

/**
 * Modo massivo: coleta TODA a configuração (template, work_around, imagem,
 * capacidade do pool via Nyx) uma única vez, mostra um resumo consolidado
 * pra aprovação, deixa excluir números específicos da lista, e só então
 * aplica a mesma configuração pra cada número incluído — sem repetir
 * perguntas por número. Pensado pro caso comum de "várias integrações pro
 * mesmo pool" (normalmente usado com Corpo com variável).
 * A chamada à Nyx fica pendente (retornada, não executada aqui) porque o
 * teste de envio deve rodar antes de colocar as integrações no pool.
 */
async function runMassBatchConfig(rl, ready, disallowTemplateConfig, summary) {
  while (true) {
    console.log('\n=== MODO MASSIVO: coleta de configuração (uma única vez) ===');

    // 1. estratégia de template + campos do pool
    const templateStrategy = await askTemplateStrategy(rl, { disallowTemplateConfig });
    let templatePoolConfig = null;
    if (templateStrategy === 'template_config_pool') {
      templatePoolConfig = await askTemplateConfigPoolSharedFields(rl);
    }

    // 2. work_around
    const wantsWorkAround = await askYesNo(rl, 'Deseja cadastrar integration_work_around para essas integrações?');
    let workAroundConfig = null;
    if (wantsWorkAround) {
      workAroundConfig = await askWorkAroundSharedFields(rl);
    }

    // 3. imagem
    const wantsImage = await askYesNo(rl, 'Deseja atualizar a imagem de perfil dessas integrações?');
    let imageReady = false;
    if (wantsImage) {
      imageReady = await confirmCredenciaisFileReady(rl);
      if (!imageReady) {
        console.log('A etapa de imagem ficará desativada nesta rodada (credenciais.txt não confirmado).');
      }
    }

    // 4. capacidade do pool via Nyx — independente da estratégia de template.
    // Se for template_config_pool, reaproveita o pool_id já coletado ali;
    // senão, pergunta o pool_id direto (mesmo padrão do "adicionar direto
    // ao pool via Nyx" do modo padrão). No massivo, todas as integrações
    // incluídas vão pro mesmo pool com a mesma capacidade.
    let nyxCapacity = null;
    let nyxPoolId = null;
    const wantsNyx = await askYesNo(rl, 'Deseja também adicionar essas integrações a um pool via Nyx?');
    if (wantsNyx) {
      nyxPoolId = templateStrategy === 'template_config_pool'
        ? templatePoolConfig.poolId
        : await askUuid(rl, 'Valor do pool_id (UUID) a usar para todas as integrações: ');

      const tamanhoDiario = await askNumber(rl, 'Valor do tamanhoDiario (aplicado a todas): ');
      const onlyMassive = await askYesNo(rl, 'onlyMassive deve ser true?');
      const onlyInsert = await askYesNo(rl, 'onlyInsert deve ser true?');
      nyxCapacity = { tamanhoDiario, onlyMassive, onlyInsert };
    }

    // 5. resumo consolidado + aprovação
    const confirmed = await confirmSummary(rl, 'Resumo da configuração massiva (será aplicada a todos os números incluídos):', {
      estrategia_template: templateStrategy,
      ...(templatePoolConfig ? {
        pool_id: templatePoolConfig.poolId,
        new_from_pool: templatePoolConfig.newFrom,
        button_url: templatePoolConfig.buttonUrl,
        new_integration_id: templatePoolConfig.newIntegrationId,
      } : {}),
      work_around: workAroundConfig ? `block_response=${workAroundConfig.blockResponse}, new_from=${workAroundConfig.newFrom}` : 'não',
      imagem: wantsImage ? (imageReady ? 'sim' : 'sim, mas será pulada (credenciais.txt não confirmado)') : 'não',
      pool_via_nyx: nyxCapacity
        ? `pool_id=${nyxPoolId}, tamanhoDiario=${nyxCapacity.tamanhoDiario}, onlyMassive=${nyxCapacity.onlyMassive}, onlyInsert=${nyxCapacity.onlyInsert}`
        : 'não',
    });

    if (!confirmed) {
      console.log('Vamos preencher a configuração massiva novamente, do zero.');
      continue;
    }

    // 6. escolher quais números excluir
    console.log('\n=== NÚMEROS QUE RECEBERÃO ESSA CONFIGURAÇÃO ===');
    ready.forEach((integration, index) => {
      console.log(`${index + 1} - ${integration.number}`);
    });
    const excludeRaw = await ask(rl, '\nDigite os números da lista que deseja EXCLUIR (separados por vírgula), ou ENTER para aplicar a todos: ');
    const excludeIndexes = excludeRaw
      .split(',')
      .map(s => parseInt(s.trim(), 10) - 1)
      .filter(n => Number.isInteger(n));

    const targetIntegrations = ready.filter((_, index) => !excludeIndexes.includes(index));
    const excludedIntegrations = ready.filter((_, index) => excludeIndexes.includes(index));

    excludedIntegrations.forEach((integration) => {
      const summaryEntry = summary.find(s => s.number === integration.number);
      if (summaryEntry) {
        summaryEntry.templateStrategy = 'excluído do modo massivo';
        summaryEntry.workAround = '-';
        summaryEntry.imagem = '-';
      }
    });

    if (!targetIntegrations.length) {
      console.log('Nenhum número selecionado. Cancelando configuração massiva.');
      return { poolConfigsCollected: [], nyxCapacity: null };
    }

    // 7. cache do asset de imagem por broker — preenchido sob demanda no
    // passo 8, na primeira vez que aparecer uma credencial de cada broker.
    // Não dá pra decidir isso adiantado pelo "type" do dados.txt: o broker
    // que importa é o da CREDENCIAL de credenciais.txt que acaba sendo
    // usada (pode ser diferente do type, ex: número marcado como 360 no
    // dados.txt mas a credencial de imagem disponível é Infobip).
    const imageAssetByBroker = {};

    // 8. aplicar pra cada número incluído
    const poolConfigsCollected = [];
    const imageResults = [];

    for (const integration of targetIntegrations) {
      console.log(`\n=== Aplicando configuração massiva em ${integration.number} ===`);
      const summaryEntry = summary.find(s => s.number === integration.number);

      if (templateStrategy === 'template_config') {
        try {
          await addTemplateToDatabase(integration);
          console.log(`✅ Template inserido/atualizado em template_config para ${integration.number}.`);
          if (summaryEntry) summaryEntry.templateStrategy = 'template_config';
        } catch (error) {
          console.error(`❌ Falha ao inserir template_config para ${integration.number}.`, error.message);
          if (summaryEntry) summaryEntry.templateStrategy = 'template_config (erro)';
        }
      } else if (templateStrategy === 'template_config_pool') {
        try {
          await addTemplateToPoolDatabase(integration, templatePoolConfig);
          console.log(`✅ Template cadastrado no template_config_pool para ${integration.number}.`);
          if (wantsNyx) {
            poolConfigsCollected.push({ integration, poolConfig: templatePoolConfig, insertStatus: 'ok' });
          }
          if (summaryEntry) {
            summaryEntry.templateStrategy = 'template_config_pool';
            summaryEntry.poolsConfigured = 1;
          }
        } catch (error) {
          console.error(`❌ Falha ao cadastrar template_config_pool para ${integration.number}.`, error.message);
          if (summaryEntry) summaryEntry.templateStrategy = 'template_config_pool (erro)';
        }
      } else if (summaryEntry) {
        summaryEntry.templateStrategy = 'nenhuma';
      }

      // Adiciona ao pool via Nyx independente da estratégia de template —
      // se for template_config_pool, o poolConfig já foi empurrado acima
      // (só em caso de sucesso do insert); template_config/nenhuma usam o
      // pool_id coletado direto no passo 4 (sem linha em template_config_pool).
      if (wantsNyx && templateStrategy !== 'template_config_pool') {
        poolConfigsCollected.push({
          integration,
          poolConfig: { poolId: nyxPoolId, newFrom: null, buttonUrl: null, newIntegrationId: null },
          insertStatus: 'não aplicável (sem template_config_pool)',
        });
        if (summaryEntry) summaryEntry.poolsConfigured = 1;
      }

      if (workAroundConfig) {
        try {
          const status = await applyWorkAround(integration, workAroundConfig);
          if (summaryEntry) summaryEntry.workAround = status === 'applied' ? 'sim' : 'não';
        } catch (error) {
          console.error(`❌ Falha no integration_work_around para ${integration.number}.`, error.message);
          if (summaryEntry) summaryEntry.workAround = 'erro';
        }
      } else if (summaryEntry) {
        summaryEntry.workAround = 'não';
      }

      if (wantsImage && imageReady) {
        try {
          const credencial = await selecionarCredencialDeImagem(rl, integration);
          if (!credencial) {
            console.log(`Nenhuma credencial selecionada para ${integration.number}, pulando imagem.`);
            if (summaryEntry) summaryEntry.imagem = 'não';
            imageResults.push({ numero: integration.number, credencial: '-', md5: '-', status: 'sem credencial' });
          } else {
            if (!imageAssetByBroker[credencial.broker]) {
              console.log(`\nEscolha a imagem a usar para credenciais do broker ${credencial.broker}:`);
              if (credencial.broker === 'INFOBIP') {
                const logoUrl = await escolherLogoInfobip(rl);
                imageAssetByBroker[credencial.broker] = { type: 'url', value: logoUrl };
              } else {
                const imagePath = await escolherImagemLocal(rl);
                if (imagePath) imageAssetByBroker[credencial.broker] = { type: 'path', value: imagePath };
              }
            }

            const asset = imageAssetByBroker[credencial.broker];
            if (!asset) {
              console.log(`Nenhuma imagem definida para o broker ${credencial.broker}, pulando.`);
              if (summaryEntry) summaryEntry.imagem = 'não';
              imageResults.push({ numero: integration.number, credencial: credencial.sender || credencial.profileCode, md5: '-', status: 'sem asset' });
            } else {
              // confirm: false — a aprovação já aconteceu uma vez no resumo
              // consolidado (passo 5), não interrompe de novo por número.
              const result = await applyProfileImageWithAsset(rl, credencial, asset, { confirm: false });
              const identificador = credencial.sender || credencial.profileCode;
              if (result.success) {
                removeUsedCredencialFromFile(credencial);
                if (summaryEntry) summaryEntry.imagem = 'sim';
                imageResults.push({ numero: integration.number, credencial: identificador, md5: result.md5, status: 'ok' });
              } else {
                if (summaryEntry) summaryEntry.imagem = 'erro';
                imageResults.push({ numero: integration.number, credencial: identificador, md5: '-', status: 'erro' });
              }
            }
          }
        } catch (error) {
          console.error(`❌ Falha na atualização de imagem para ${integration.number}.`, error.message);
          if (summaryEntry) summaryEntry.imagem = 'erro';
          imageResults.push({ numero: integration.number, credencial: '-', md5: '-', status: 'erro' });
        }
      } else if (summaryEntry) {
        summaryEntry.imagem = 'não';
      }
    }

    if (imageResults.length) {
      console.log('\n=== RESUMO DAS IMAGENS APLICADAS (modo massivo) ===');
      console.table(imageResults);
    }

    return { poolConfigsCollected, nyxCapacity };
  }
}

/**
 * Aplica a adição ao pool via Nyx coletada no modo massivo, com a
 * capacidade já definida na coleta (sem perguntar de novo) — chamada
 * depois do teste de envio, mesma regra de "testa, valida, depois coloca
 * no pool" do modo padrão.
 */
async function applyMassNyxPool(poolConfigsCollected, nyxCapacity, summary) {
  console.log('\n=== MODO MASSIVO: pool via Nyx ===');

  if (!nyxCapacity || !poolConfigsCollected.length) {
    console.log('Nada a adicionar ao pool via Nyx nesta rodada.');
    return;
  }

  for (const entry of poolConfigsCollected) {
    const { integration, poolConfig } = entry;
    try {
      await addIntegrationToNyxPool({
        integrationId: integration.integrationId,
        poolId: poolConfig.poolId,
        broker: resolveBrokerForNyx(integration.type),
        tamanhoDiario: nyxCapacity.tamanhoDiario,
        onlyMassive: nyxCapacity.onlyMassive,
        onlyInsert: nyxCapacity.onlyInsert,
      });
      console.log(`✅ ${integration.number} adicionado ao pool ${poolConfig.poolId} via Nyx.`);
      entry.nyx = { ...nyxCapacity, status: 'ok' };
    } catch (error) {
      console.error(`❌ Falha ao adicionar ${integration.number} ao pool via Nyx.`, error?.response?.data || error.message);
      entry.nyx = { ...nyxCapacity, status: 'erro' };
    }
  }
}

/**
 * Fase 6: grava no DynamoDB (automática, controlada por wantsToWriteToDynamo).
 */
async function phaseDynamo(ready, summary) {
  console.log('\n=== ETAPA 8: DynamoDB ===');
  for (const integration of ready) {
    const summaryEntry = summary.find(s => s.number === integration.number);
    if (!wantsToWriteToDynamo) {
      if (summaryEntry) summaryEntry.dynamo = 'pulado';
      continue;
    }
    try {
      await writeToDynamoDB(integration.integrationId, integration.token);
      console.log(`✅ ${integration.number}: dados gravados no DynamoDB.`);
      if (summaryEntry) summaryEntry.dynamo = 'ok';
    } catch (error) {
      console.error(`❌ ${integration.number}: falha ao gravar no DynamoDB.`, error.message);
      if (summaryEntry) summaryEntry.dynamo = 'erro';
    }
  }
}

/**
 * Fase 7: dispara o teste de envio (SQS ou sendMessage), automática,
 * controlada por wantToTest.
 */
async function phaseTest(ready, bodyType, summary) {
  console.log('\n=== ETAPA 5: teste de envio ===');
  for (const integration of ready) {
    const summaryEntry = summary.find(s => s.number === integration.number);
    if (!wantToTest) {
      if (summaryEntry) summaryEntry.teste = 'pulado';
      continue;
    }
    try {
      let wasDelivered;
      if (integration.lang === 'en') {
        wasDelivered = await sendToSQS(integration.integrationId, "TESTE_WEBHOOK_EN");
      } else if (integration.lang === 'pt') {
        wasDelivered = await sendToSQS(integration.integrationId, "TESTE_WEBHOOK_PT");
      } else {
        wasDelivered = await sendMessage(integration.integrationId, integration.templateName, integration.hasParam, bodyType);
      }
      integration.wasDelivered = wasDelivered;
      console.log(`✅ ${integration.number}: teste de envio disparado.`);
      if (summaryEntry) summaryEntry.teste = 'disparado';
    } catch (error) {
      console.error(`❌ ${integration.number}: falha no teste de envio.`, error.message);
      if (summaryEntry) summaryEntry.teste = 'erro';
    }
  }
}

/**
 * Fase 8: imprime as tabelas resumo (console.table) e salva em
 * resumo-onboarding.json — como um "select" mostrando tudo que foi feito.
 */
function printAndSaveSummary(summary, poolConfigsCollected) {
  console.log('\n=== RESUMO DO ONBOARDING COMPLETO ===');
  console.table(summary.map(s => ({
    'Número': s.number,
    IntegrationId: s.integrationId,
    'Criação': s.criacao,
    'Estratégia template': s.templateStrategy,
    'Pools configurados': s.poolsConfigured,
    WorkAround: s.workAround,
    Imagem: s.imagem,
    DynamoDB: s.dynamo,
    Teste: s.teste,
  })));

  let poolRows = [];
  if (poolConfigsCollected.length) {
    poolRows = poolConfigsCollected.map(({ integration, poolConfig, insertStatus, nyx }) => ({
      'Número': integration.number,
      pool_id: poolConfig.poolId,
      new_from: poolConfig.newFrom,
      button_url: poolConfig.buttonUrl,
      new_integration_id: poolConfig.newIntegrationId,
      template_config_pool: insertStatus,
      Nyx: nyx ? nyx.status : 'não solicitado',
      tamanhoDiario: nyx ? nyx.tamanhoDiario : '-',
      onlyMassive: nyx ? nyx.onlyMassive : '-',
      onlyInsert: nyx ? nyx.onlyInsert : '-',
    }));

    console.log('\n=== POOLS CONFIGURADOS (template_config_pool) ===');
    console.table(poolRows);
  }

  const output = {
    geradoEm: new Date().toISOString(),
    integracoes: summary,
    pools: poolRows,
  };
  fs.writeFileSync('resumo-onboarding.json', JSON.stringify(output, null, 2));
  console.log('\nResumo salvo em resumo-onboarding.json');
}

/**
 * Fluxo completo de onboarding, organizado em fases sequenciais — cada fase
 * passa por TODOS os números do dados.txt antes de seguir pra próxima. Isso
 * deixa claro o que está sendo perguntado/feito pra cada número, e permite
 * mais de um template_config_pool (e pool via Nyx) por integração.
 * Se uma fase falhar pra uma integração específica, o erro é logado e ela
 * segue nas próximas fases; só quem falha na fase 1 (criar/editar) é
 * excluído do resto do fluxo, pois não dá pra continuar sem integrationId.
 */
async function runOnboardingCompleto(rl, bodyType = testBodyType) {
  const integrations = await processIntegrationData('./dados.txt');

  const disallowTemplateConfig = bodyType === TEST_BODY_TYPES.BUTTON_URL_VAR;
  if (disallowTemplateConfig) {
    console.log('\nbodyType BUTTON_URL_VAR selecionado: a opção template_config não estará disponível (obrigatório para esse tipo de teste). template_config_pool continua disponível normalmente.');
  }

  const allowDirectNyxPool = bodyType === TEST_BODY_TYPES.BODY_VAR;

  const { ready, summary } = await phaseCreateOrUpdateAll(integrations);

  if (!ready.length) {
    console.log('\nNenhuma integração pôde ser criada/editada. Encerrando onboarding completo.');
    printAndSaveSummary(summary, []);
    return summary;
  }

  const mode = await askOnboardingMode(rl);

  let poolConfigsCollected = [];
  let massNyxCapacity = null;

  if (mode === 'mass') {
    const massResult = await runMassBatchConfig(rl, ready, disallowTemplateConfig, summary);
    poolConfigsCollected = massResult.poolConfigsCollected;
    massNyxCapacity = massResult.nyxCapacity;
  } else {
    const strategyResult = await phaseTemplateStrategy(rl, ready, disallowTemplateConfig, summary);
    poolConfigsCollected = strategyResult.poolConfigsCollected;
    await phaseWorkAround(rl, ready, summary);
    await phaseImage(rl, ready, summary);
  }

  await phaseTest(ready, bodyType, summary);

  if (mode === 'standard' && allowDirectNyxPool) {
    const extraPoolConfigs = await phaseDirectNyxPool(rl, ready, summary);
    poolConfigsCollected.push(...extraPoolConfigs);
  }

  if (mode === 'mass') {
    await applyMassNyxPool(poolConfigsCollected, massNyxCapacity, summary);
  } else {
    await phaseNyxPool(rl, poolConfigsCollected, summary);
  }

  await phaseDynamo(ready, summary);

  const results = ready.map(integration => ({
    id: integration.integrationId,
    accountName: integration.accountName,
    token: integration.token,
    templateName: integration.templateName || 'Default Template',
    metaParams: integration.metaParams ? integration.metaParams : [],
    wasDelivered: integration.wasDelivered || '',
  }));
  fs.writeFileSync("integracoes-criadas.json", JSON.stringify(results, null, 2));

  printAndSaveSummary(summary, poolConfigsCollected);

  return summary;
}

const readline = require('readline');

function createRL() {
  return readline.createInterface({ input: process.stdin, output: process.stdout });

}

function ask(rl, question) {
  return new Promise(resolve => rl.question(question, answer => resolve(answer.trim())));
}

async function chooseBodyType(rl) {
  const map = {
    '1': TEST_BODY_TYPES.BUTTON_URL_VAR,
    '2': TEST_BODY_TYPES.INFOBIP_2_BUTTONS,
    '3': TEST_BODY_TYPES.BODY_VAR,
    '4': TEST_BODY_TYPES.BODY_NO_VAR,
  };

  let bodyType = null;
  while (!bodyType) {
    console.log('\n=== O QUE VOCÊ QUER TESTAR? ===');
    console.log('1 - Botão com variável (URL)');
    console.log('2 - 2 botões quick_reply (Infobip)');
    console.log('3 - Corpo com variável');
    console.log('4 - Corpo sem variável');

    const raw = await ask(rl, '\nEscolha uma opção: ');
    const opt = raw.replace(/\D/g, '');
    bodyType = map[opt];

    if (!bodyType) {
      console.log(`Opção inválida ("${raw}"). Tente novamente.`);
    }
  }

  console.log(`Tipo selecionado: ${bodyType}`);
  return bodyType;
}

async function selecionarIntegracaoDoDados(rl) {
  const integrations = await processIntegrationData('./dados.txt');

  if (!integrations.length) {
    console.log('\nNenhuma integração encontrada em ./dados.txt.');
    return null;
  }

  console.log('\n=== INTEGRAÇÕES DISPONÍVEIS (dados.txt) ===');
  integrations.forEach((integ, index) => {
    console.log(`${index + 1} - ${integ.number} | template: ${integ.templateName} | integrationId: ${integ.integrationId}`);
  });

  let selecionada = null;
  while (!selecionada) {
    const raw = await ask(rl, '\nEscolha o número da integração: ');
    const idx = parseInt(raw.replace(/\D/g, ''), 10) - 1;

    if (Number.isInteger(idx) && integrations[idx]) {
      selecionada = integrations[idx];
    } else {
      console.log(`Opção inválida ("${raw}"). Tente novamente.`);
    }
  }

  return selecionada;
}

/**
 * Pra Infobip, o "sender" em credenciais.txt É o próprio número (formato
 * igual ao de dados.txt) — dá pra casar automático. 360/META usam
 * profileCode (um ID interno, não o número), então não tem como.
 */
function findCredencialForIntegration(credenciais, integration) {
  if (!integration || integration.type !== 'INFOBIP') return null;
  return credenciais.find(
    (cred) => cred.broker === 'INFOBIP' && cred.sender?.trim() === integration.number?.trim()
  ) || null;
}

/**
 * Escolhe a credencial de imagem. Se `integration` for passada e for do
 * tipo INFOBIP, tenta casar automaticamente pelo número (sender ===
 * integration.number) e nem pergunta nesse caso. Senão, cai pra seleção
 * manual da lista (comportamento de sempre).
 */
async function selecionarCredencialDeImagem(rl, integration = null) {
  const credenciais = readCredenciais(CREDENCIAIS_FILE_PATH);

  if (!credenciais.length) {
    console.log(`\nNenhuma credencial válida encontrada em ${CREDENCIAIS_FILE_PATH}.`);
    return null;
  }

  const autoMatch = findCredencialForIntegration(credenciais, integration);
  if (autoMatch) {
    console.log(`\nCredencial encontrada automaticamente para ${integration.number}: Infobip | sender: ${autoMatch.sender}`);
    return autoMatch;
  }

  console.log(`\n=== CREDENCIAIS DISPONÍVEIS (${CREDENCIAIS_FILE_PATH}) ===`);
  credenciais.forEach((cred, index) => {
    const label = cred.broker === 'INFOBIP'
      ? `Infobip | sender: ${cred.sender}`
      : `${cred.broker} | profileCode: ${cred.profileCode}`;
    console.log(`${index + 1} - ${label}`);
  });

  let selecionada = null;
  while (!selecionada) {
    const raw = await ask(rl, '\nEscolha o número da credencial: ');
    const idx = parseInt(raw.replace(/\D/g, ''), 10) - 1;

    if (Number.isInteger(idx) && credenciais[idx]) {
      selecionada = credenciais[idx];
    } else {
      console.log(`Opção inválida ("${raw}"). Tente novamente.`);
    }
  }

  return selecionada;
}

async function escolherLogoInfobip(rl) {
  console.log('\n=== LOGOS DISPONÍVEIS (Infobip) ===');
  INFOBIP_LOGO_OPTIONS.forEach((logo, index) => {
    console.log(`${index + 1} - ${logo.label}`);
  });

  let selecionado = null;
  while (!selecionado) {
    const raw = await ask(rl, '\nEscolha o número do logo: ');
    const idx = parseInt(raw.replace(/\D/g, ''), 10) - 1;

    if (Number.isInteger(idx) && INFOBIP_LOGO_OPTIONS[idx]) {
      selecionado = INFOBIP_LOGO_OPTIONS[idx];
    } else {
      console.log(`Opção inválida ("${raw}"). Tente novamente.`);
    }
  }

  return selecionado.url;
}

async function escolherImagemLocal(rl) {
  const imagens = readImagensLocais(IMAGENS_LOCAIS_FILE_PATH);

  if (!imagens.length) {
    console.log(`\nNenhuma imagem cadastrada em ${IMAGENS_LOCAIS_FILE_PATH}.`);
    console.log('Cadastre no formato "nome, caminho/da/imagem.jpg" (uma por linha).');
    return null;
  }

  console.log(`\n=== IMAGENS DISPONÍVEIS (${IMAGENS_LOCAIS_FILE_PATH}) ===`);
  imagens.forEach((img, index) => {
    console.log(`${index + 1} - ${img.label}`);
  });

  let selecionada = null;
  while (!selecionada) {
    const raw = await ask(rl, '\nEscolha o número da imagem: ');
    const idx = parseInt(raw.replace(/\D/g, ''), 10) - 1;

    if (Number.isInteger(idx) && imagens[idx]) {
      selecionada = imagens[idx];
    } else {
      console.log(`Opção inválida ("${raw}"). Tente novamente.`);
    }
  }

  return selecionada.path;
}

async function menuTestarIntegracao(rl) {
  const integracao = await selecionarIntegracaoDoDados(rl);
  if (!integracao) return;

  const bodyType = await chooseBodyType(rl);

  if (bodyType === TEST_BODY_TYPES.BUTTON_URL_VAR && addTemplateConfig) {
    console.log('\naddTemplateConfig estava true; ajustando para false automaticamente (obrigatório para esse tipo de teste).');
    addTemplateConfig = false;
  }

  console.log(`\nEnviando teste (${bodyType}) para ${integracao.number} (integrationId: ${integracao.integrationId})...`);
  await sendMessage(integracao.integrationId, integracao.templateName, integracao.hasParam, bodyType);
  console.log('Teste concluído.');
}

/**
 * Aplica a foto de perfil para a credencial selecionada (broker já resolvido).
 * Pede o logo/imagem de acordo com o broker e chama a API correspondente.
 * Retorna true se a imagem foi de fato aplicada, false caso contrário
 * (erro ou usuário abandonou a seleção de imagem).
 */
/**
 * Aplica a foto de perfil pra uma credencial usando um asset JÁ escolhido
 * (URL, pro Infobip, ou path local, pro 360/META) — não pergunta qual
 * imagem usar, só gera a variação randomizada, confirma e envia. Permite
 * reaproveitar a MESMA imagem escolhida pra vários números (modo massivo).
 */
/**
 * `confirm = false` pula a confirmação individual (usado no modo massivo,
 * onde a aprovação já aconteceu uma vez no resumo consolidado — não faz
 * sentido confirmar de novo pra cada número). Retorna { success, md5 }
 * pra quem chama montar um resumo único no final, em vez de interromper
 * por integração.
 */
async function applyProfileImageWithAsset(rl, credencial, asset, { confirm = true } = {}) {
  try {
    console.log('\nGerando uma variação única da imagem (nunca reenvia a mesma)...');

    if (credencial.broker === 'INFOBIP') {
      const baseBuffer = await downloadImageBuffer(asset.value);
      const randomized = await randomizeAndUploadImage(baseBuffer);
      console.log(`Variação gerada (md5: ${randomized.md5}) e subida em: ${randomized.publicUrl}`);

      if (confirm) {
        const confirmed = await confirmSummary(rl, 'Resumo da atualização de imagem:', {
          broker: 'INFOBIP',
          sender: credencial.sender,
          logoBase: asset.value,
          logoRandomizado: randomized.publicUrl,
        });
        if (!confirmed) {
          console.log('Atualização de imagem cancelada.');
          return { success: false, md5: null };
        }
      }
      console.log(`\nAtualizando foto de perfil (Infobip) para ${credencial.sender}...`);
      const data = await sendInfobipProfilePhoto(credencial.sender, randomized.publicUrl);
      console.log('✅ Sucesso:', data);
      return { success: true, md5: randomized.md5 };

    } else if (credencial.broker === '360') {
      const baseBuffer = await fs.promises.readFile(asset.value);
      const randomized = await randomizeOnce(baseBuffer);
      console.log(`Variação gerada (md5: ${randomized.md5}).`);

      if (confirm) {
        const confirmed = await confirmSummary(rl, 'Resumo da atualização de imagem:', {
          broker: '360',
          profileCode: credencial.profileCode,
          imagemBase: asset.value,
          md5Randomizado: randomized.md5,
        });
        if (!confirmed) {
          console.log('Atualização de imagem cancelada.');
          return { success: false, md5: null };
        }
      }
      console.log(`\nSubindo imagem (360dialog) para ${credencial.profileCode}...`);
      const handle = await upload360Image(randomized.buffer, credencial.token);
      const data = await send360ProfilePhoto(credencial.token, credencial.profileCode, handle);
      console.log('✅ Sucesso:', data);
      return { success: true, md5: randomized.md5 };

    } else if (credencial.broker === 'META') {
      const baseBuffer = await fs.promises.readFile(asset.value);
      const randomized = await randomizeOnce(baseBuffer);
      console.log(`Variação gerada (md5: ${randomized.md5}).`);

      if (confirm) {
        const confirmed = await confirmSummary(rl, 'Resumo da atualização de imagem:', {
          broker: 'META',
          profileCode: credencial.profileCode,
          imagemBase: asset.value,
          md5Randomizado: randomized.md5,
        });
        if (!confirmed) {
          console.log('Atualização de imagem cancelada.');
          return { success: false, md5: null };
        }
      }
      console.log(`\nSubindo imagem (META) para ${credencial.profileCode}...`);
      const handle = await uploadMetaImage(randomized.buffer, credencial.token, credencial.appId);
      const data = await sendMetaProfilePhoto(credencial.token, credencial.profileCode, handle);
      console.log('✅ Sucesso:', data);
      return { success: true, md5: randomized.md5 };
    }

    return { success: false, md5: null };
  } catch (error) {
    console.error('❌ Erro ao atualizar imagem:', error?.response?.data || error.message);
    return { success: false, md5: null };
  }
}

/**
 * Escolhe a imagem interativamente (logo pro Infobip, arquivo local pro
 * 360/META) e aplica — comportamento igual ao de sempre, usado no menu
 * avulso e no fluxo padrão (número a número).
 */
async function applyProfileImage(rl, credencial) {
  let asset;

  if (credencial.broker === 'INFOBIP') {
    const logoUrl = await escolherLogoInfobip(rl);
    asset = { type: 'url', value: logoUrl };
  } else if (credencial.broker === '360' || credencial.broker === 'META') {
    const imagePath = await escolherImagemLocal(rl);
    if (!imagePath) return false;
    asset = { type: 'path', value: imagePath };
  } else {
    return false;
  }

  const result = await applyProfileImageWithAsset(rl, credencial, asset);
  return result.success;
}

/**
 * Remove só a linha da credencial que acabou de ser usada em credenciais.txt,
 * mantendo as demais intactas — evita reaproveitar por engano a mesma
 * credencial numa próxima vez, sem atrapalhar outras integrações que ainda
 * serão processadas no mesmo loop.
 */
function removeUsedCredencialFromFile(credencial) {
  if (!credencial.rawLine || !fs.existsSync(CREDENCIAIS_FILE_PATH)) return;

  const lines = fs.readFileSync(CREDENCIAIS_FILE_PATH, 'utf8').split('\n');
  const idx = lines.indexOf(credencial.rawLine);
  if (idx === -1) return;

  lines.splice(idx, 1);
  fs.writeFileSync(CREDENCIAIS_FILE_PATH, lines.join('\n'));
  console.log(`Credencial usada removida de ${CREDENCIAIS_FILE_PATH} (evita reaproveitar por engano).`);
}

/**
 * Confirma com o usuário se as informações da credencial de imagem já foram
 * adicionadas em credenciais.txt antes de deixar seguir pra seleção.
 */
async function confirmCredenciaisFileReady(rl) {
  const jaAdicionou = await askYesNo(
    rl,
    `Você já adicionou as informações da imagem em ${CREDENCIAIS_FILE_PATH}?`
  );

  if (!jaAdicionou) {
    console.log(`Adicione as informações da imagem em ${CREDENCIAIS_FILE_PATH} antes de continuar. Pulando etapa de imagem.`);
    return false;
  }

  return true;
}

/**
 * Pergunta, para UMA integração, se deseja atualizar a foto de perfil dela.
 * credenciais.txt é uma lista independente de dados.txt (sem vínculo por
 * número), então a credencial certa é sempre escolhida manualmente.
 */
async function handleIntegrationImageUpdate(rl, integration) {
  const wantsImage = await askYesNo(
    rl,
    `Deseja atualizar a imagem de perfil da integração ${integration.number}?`
  );

  if (!wantsImage) {
    console.log('Pulando atualização de imagem para esta integração.');
    return 'skipped';
  }

  const isReady = await confirmCredenciaisFileReady(rl);
  if (!isReady) return 'skipped';

  const credencial = await selecionarCredencialDeImagem(rl, integration);
  if (!credencial) {
    console.log('Nenhuma credencial encontrada/selecionada, pulando atualização de imagem.');
    return 'skipped';
  }

  const wasApplied = await applyProfileImage(rl, credencial);
  if (wasApplied) {
    removeUsedCredencialFromFile(credencial);
    return 'applied';
  }
  return 'failed';
}

async function menuAdicionarImagem(rl) {
  const isReady = await confirmCredenciaisFileReady(rl);
  if (!isReady) return;

  const credencial = await selecionarCredencialDeImagem(rl);
  if (!credencial) return;

  const wasApplied = await applyProfileImage(rl, credencial);
  if (wasApplied) {
    removeUsedCredencialFromFile(credencial);
  }

  console.log('\nProcesso de imagem concluído.');
}

async function menuOnboardingIntegração(rl) {
  console.log('\n=== ONBOARDING INTEGRAÇÃO ===');
  console.log('Esse fluxo lê ./dados.txt, cria as integrações, salva no DynamoDB e testa o envio.');

  const bodyType = await chooseBodyType(rl);

  if (bodyType === TEST_BODY_TYPES.BUTTON_URL_VAR && addTemplateConfig) {
    console.log('\naddTemplateConfig estava true; ajustando para false automaticamente (obrigatório para esse tipo de teste).');
    addTemplateConfig = false;
  }

  console.log('\nIniciando onboarding completo...');
  await createIntegration(bodyType);
  console.log('\nOnboarding completo finalizado. Veja o arquivo integracoes-criadas.json');
}

async function menuOnboardingCompleto(rl) {
  console.log('\n=== ONBOARDING COMPLETO ===');
  console.log('Fluxo em fases. Depois de criar/editar, escolhe o MODO:');
  console.log('  1) Massivo — coleta template/work_around/imagem/capacidade do pool via Nyx UMA VEZ,');
  console.log('     mostra resumo pra aprovar e deixa excluir números, aplica em todos os incluídos.');
  console.log('     Bom pra várias integrações indo pro mesmo pool.');
  console.log('  2) Padrão — pergunta número a número, como sempre foi (permite múltiplos pools por');
  console.log('     número, e só com "Corpo com variável" oferece adicionar direto ao pool via Nyx).');
  console.log('Em ambos: teste de envio roda antes de colocar no pool -> DynamoDB -> tabela resumo.');

  const bodyType = await chooseBodyType(rl);

  console.log('\nIniciando onboarding completo...');
  await runOnboardingCompleto(rl, bodyType);
  console.log('\nOnboarding completo finalizado. Veja o arquivo integracoes-criadas.json');
}

async function mainMenu() {
  const rl = createRL();
  let exit = false;

  while (!exit) {
    console.log('\n=== MENU PRINCIPAL ===');
    console.log('1 - Testar integração');
    console.log('2 - Onboarding integração');
    console.log('3 - Adicionar imagem');
    console.log('4 - Onboarding completo');
    console.log('0 - Sair');

    const option = await ask(rl, '\nEscolha uma opção: ');

    switch (option) {
      case '1':
        await menuTestarIntegracao(rl);
        break;
      case '2':
        await menuOnboardingIntegração(rl);
        break;
      case '3':
        await menuAdicionarImagem(rl);
        break;
      case '4':
        await menuOnboardingCompleto(rl);
        break;
      case '0':
        exit = true;
        break;
      default:
        console.log('Opção inválida.');
    }
  }

  console.log('\nAté mais!');
  rl.close();
}

mainMenu();