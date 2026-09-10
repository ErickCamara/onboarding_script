require('dotenv').config();
const { Pool } = require('pg');

// Antes eram 3 conexoes (waMeta, waInfoBip, wa360), uma para cada broker,
// todas apontando para bases com o mesmo schema (template_config,
// integration_work_around, template_config_pool). Agora esses 3 bancos
// foram unificados em um so, o whatsapp_config, entao basta uma conexao.
const whatsappConfig = new Pool({
  host: process.env.WHATSAPP_CONFIG_DB_HOST,
  port: process.env.WHATSAPP_CONFIG_DB_PORT,
  database: process.env.WHATSAPP_CONFIG_DB_DATABASE,
  user: process.env.WHATSAPP_CONFIG_DB_USERNAME,
  password: process.env.WHATSAPP_CONFIG_DB_PASSWORD,
  ssl: {
    rejectUnauthorized: false
  }
});

// Continua separado: base de dados diferente (data lake de mensagens),
// nao faz parte da consolidacao do whatsapp_config.
const lake = new Pool({
  host: process.env.LAKE_DB_HOST,
  port: process.env.LAKE_DB_PORT,
  database: process.env.LAKE_DB_DATABASE,
  user: process.env.LAKE_DB_USERNAME,
  password: process.env.LAKE_DB_PASSWORD,
  ssl: {
    rejectUnauthorized: false
  }
});

// ==========================================================================
// template_config
// Antes replicada nas 3 bases (META / 360 / INFOBIP), cada uma com sua
// propria funcao (waMetaDatabase, waInfoBipDatabase, wa360Database).
// Agora tudo mora no banco unico whatsapp_config, entao uma funcao so
// resolve os 3 casos.
// ==========================================================================
async function templateConfigDatabase({ integrationId, name, metaParams, language }) {
  const checkQuery = `
    SELECT id FROM public.template_config WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await whatsappConfig.query(checkQuery, [integrationId]);

    if (existingRows.length > 0) {
      const updateQuery = `
        UPDATE public.template_config
        SET 
            name = $1,
            last_updated_date = NOW(),
            meta_params = $2,
            language = $4
        WHERE id = (
            SELECT id
            FROM public.template_config
            WHERE integration_id = $3::uuid
            LIMIT 1
        )
        RETURNING *;
      `;

      const { rows } = await whatsappConfig.query(updateQuery, [name, JSON.stringify(metaParams), integrationId, language]);
      console.log('Updated template config:', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.template_config (id, "type", name, integration_id, created_date, last_updated_date, status, use_param, use_meta_param, meta_params, language)
        VALUES(uuid_generate_v4(), 'TEXT', $1, $2::uuid, NOW(), NOW(), 'ACTIVE', false, true, $3, $4)
        RETURNING *;
      `;

      const { rows } = await whatsappConfig.query(insertQuery, [name, integrationId, JSON.stringify(metaParams), language]);
      console.log('Inserted template config:', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in templateConfigDatabase function:', error);
    throw new Error(`Error in templateConfigDatabase function: ${error.message}`);
  }
}

async function lakeDatabase({ controlId }) {
  const query = `
  select * from messages m where control_id = $1
  `;

  try {
    const { rows } = await lake.query(query, [controlId]);
    // console.log('rows: ', rows);

    return rows;
  } catch (error) {
    console.error('Error inserting meta template config:', error);
    throw new Error(`Error inserting meta template config: ${error.message}`);
  }
}

// ==========================================================================
// integration_work_around
// Antes replicada nas 3 bases (META / 360 / INFOBIP), mesmo padrao de
// SELECT antes de decidir entre UPDATE (ja existe pra essa integracao) e
// INSERT. Agora tudo mora no banco unico whatsapp_config.
// ==========================================================================
async function integrationWorkAroundDatabase({ integrationId, newFrom, blockResponse }) {
  const checkQuery = `
    SELECT id FROM public.integration_work_around WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await whatsappConfig.query(checkQuery, [integrationId]);

    if (existingRows.length > 0) {
      const updateQuery = `
        UPDATE public.integration_work_around
        SET
            new_from = $1,
            block_response = $2,
            updated_at = NOW()
        WHERE id = (
            SELECT id
            FROM public.integration_work_around
            WHERE integration_id = $3::uuid
            LIMIT 1
        )
        RETURNING *;
      `;

      const { rows } = await whatsappConfig.query(updateQuery, [newFrom, blockResponse, integrationId]);
      console.log('Updated integration_work_around:', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.integration_work_around
        (id, integration_id, new_from, created_at, updated_at, description, block_response)
        VALUES(uuid_generate_v4(), $1::uuid, $2, NOW(), NULL, NULL, $3)
        RETURNING *;
      `;

      const { rows } = await whatsappConfig.query(insertQuery, [integrationId, newFrom, blockResponse]);
      console.log('Inserted integration_work_around:', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in integrationWorkAroundDatabase function:', error);
    throw new Error(`Error in integrationWorkAroundDatabase function: ${error.message}`);
  }
}

// ==========================================================================
// template_config_pool
// Antes replicada nas 3 bases (META / 360 / INFOBIP). Apenas INSERT (sem
// checagem de existencia previa) — cada chamada cria uma nova linha na
// pool. Agora tudo mora no banco unico whatsapp_config.
// Campos fixos seguem o padrao observado: type='TEXT', status='ACTIVE',
// use_meta_param=true, meta_params='[]', image_url=NULL, language='pt_BR',
// template_type='UTILITY'.
// ==========================================================================
async function templateConfigPoolDatabase({ integrationId, name, newFrom, poolId, buttonUrl, newIntegrationId }) {
  const insertQuery = `
    INSERT INTO public.template_config_pool
    (id, "type", name, status, use_meta_param, meta_params, image_url, "language", template_type, new_from, created_date, last_updated_date, pool_id, button_url, new_integration_id, integration_id)
    VALUES(uuid_generate_v4(), 'TEXT', $1, 'ACTIVE', true, '[]'::jsonb, NULL, 'pt_BR', 'UTILITY', $2, NOW(), NOW(), $3::uuid, $4, $5::uuid, $6::uuid)
    RETURNING *;
  `;

  try {
    const { rows } = await whatsappConfig.query(insertQuery, [name, newFrom, poolId, buttonUrl, newIntegrationId, integrationId]);
    console.log('Inserted template_config_pool:', rows[0]);
    return rows[0];
  } catch (error) {
    console.error('Error in templateConfigPoolDatabase function:', error);
    throw new Error(`Error in templateConfigPoolDatabase function: ${error.message}`);
  }
}

// ==========================================================================
// Leituras usadas pela etapa de "duplicar configuração": pegam a config
// atual de uma integração já configurada (integration_work_around e todas
// as linhas de template_config_pool) pra replicar em números novos.
// ==========================================================================
async function getIntegrationWorkAround({ integrationId }) {
  const query = `SELECT * FROM public.integration_work_around WHERE integration_id = $1::uuid LIMIT 1;`;

  try {
    const { rows } = await whatsappConfig.query(query, [integrationId]);
    return rows[0] || null;
  } catch (error) {
    console.error('Error in getIntegrationWorkAround function:', error);
    throw new Error(`Error in getIntegrationWorkAround function: ${error.message}`);
  }
}

async function getTemplateConfigPoolRows({ integrationId }) {
  const query = `SELECT * FROM public.template_config_pool WHERE integration_id = $1::uuid;`;

  try {
    const { rows } = await whatsappConfig.query(query, [integrationId]);
    return rows;
  } catch (error) {
    console.error('Error in getTemplateConfigPoolRows function:', error);
    throw new Error(`Error in getTemplateConfigPoolRows function: ${error.message}`);
  }
}

module.exports = {
  templateConfigDatabase,
  lakeDatabase,
  integrationWorkAroundDatabase,
  templateConfigPoolDatabase,
  getIntegrationWorkAround,
  getTemplateConfigPoolRows
}
