require('dotenv').config();
const { Pool } = require('pg');

const waMeta = new Pool({
  host: process.env.WAMETA_DB_HOST,
  port: process.env.WAMETA_DB_PORT,
  database: process.env.WAMETA_DB_DATABASE,
  user: process.env.WAMETA_DB_USERNAME,
  password: process.env.WAMETA_DB_PASSWORD,
  ssl: {
    rejectUnauthorized: false
  }
});

const waInfoBip = new Pool({
  host: process.env.WAINFOBIP_DB_HOST,
  port: process.env.WAINFOBIP_DB_PORT,
  database: process.env.WAINFOBIP_DB_DATABASE,
  user: process.env.WAINFOBIP_DB_USERNAME,
  password: process.env.WAINFOBIP_DB_PASSWORD,
  ssl: {
    rejectUnauthorized: false
  }
});

const wa360 = new Pool({
  host: process.env.WA360_DB_HOST,
  port: process.env.WA360_DB_PORT,
  database: process.env.WA360_DB_DATABASE,
  user: process.env.WA360_DB_USERNAME,
  password: process.env.WA360_DB_PASSWORD,
  ssl: {
    rejectUnauthorized: false
  }
});

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

async function waMetaDatabase({ integrationId, name, metaParams, language }) {
  const checkQuery = `
    SELECT id FROM public.template_config WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await waMeta.query(checkQuery, [integrationId]);

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

      const { rows } = await waMeta.query(updateQuery, [name, JSON.stringify(metaParams), integrationId, language]);
      console.log('Updated meta template config:', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.template_config (id, "type", name, integration_id, created_date, last_updated_date, status, use_param, use_meta_param, meta_params, language)
        VALUES(uuid_generate_v4(), 'TEXT', $1, $2::uuid, NOW(), NOW(), 'ACTIVE', false, true, $3, $4)
        RETURNING *;
      `;

      const { rows } = await waMeta.query(insertQuery, [name, integrationId, JSON.stringify(metaParams), language]);
      console.log('Inserted meta template config:', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in waMetaDatabase function:', error);
    throw new Error(`Error in waMetaDatabase function: ${error.message}`);
  }
}

async function waInfoBipDatabase({ integrationId, name, metaParams, language }) {
  const checkQuery = `
    SELECT id FROM public.template_config WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await waInfoBip.query(checkQuery, [integrationId]);

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

      const { rows } = await waInfoBip.query(updateQuery, [name, JSON.stringify(metaParams), integrationId, language]);
      console.log('Updated meta template config:', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.template_config (id, "type", name, integration_id, created_date, last_updated_date, status, use_param, use_meta_param, meta_params, language)
        VALUES(uuid_generate_v4(), 'TEXT', $1, $2::uuid, NOW(), NOW(), 'ACTIVE', false, true, $3, $4)
        RETURNING *;
      `;

      const { rows } = await waInfoBip.query(insertQuery, [name, integrationId, JSON.stringify(metaParams), language]);
      console.log('Inserted meta template config:', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in waInfoBipDatabase function:', error);
    throw new Error(`Error in waInfoBipDatabase function: ${error.message}`);
  }
}

async function wa360Database({ integrationId, name, metaParams, language }) {
  const checkQuery = `
    SELECT id FROM public.template_config WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await wa360.query(checkQuery, [integrationId]);

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

      const { rows } = await waMeta.query(updateQuery, [name, JSON.stringify(metaParams), integrationId, language]);
      console.log('Updated meta template config:', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.template_config (id, "type", name, integration_id, created_date, last_updated_date, status, use_param, use_meta_param, meta_params, language)
        VALUES(uuid_generate_v4(), 'TEXT', $1, $2::uuid, NOW(), NOW(), 'ACTIVE', false, true, $3, $4)
        RETURNING *;
      `;

      const { rows } = await wa360.query(insertQuery, [name, integrationId, JSON.stringify(metaParams), language]);
      console.log('Inserted meta template config:', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in wa360Database function:', error);
    throw new Error(`Error in wa360Database function: ${error.message}`);
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
// Replicada nas 3 bases (META / 360 / INFOBIP), mesmo padrão de SELECT
// antes de decidir entre UPDATE (já existe pra essa integração) e INSERT.
// ==========================================================================

async function waMetaWorkAroundDatabase({ integrationId, newFrom, blockResponse }) {
  const checkQuery = `
    SELECT id FROM public.integration_work_around WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await waMeta.query(checkQuery, [integrationId]);

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

      const { rows } = await waMeta.query(updateQuery, [newFrom, blockResponse, integrationId]);
      console.log('Updated integration_work_around (META):', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.integration_work_around
        (id, integration_id, new_from, created_at, updated_at, description, block_response)
        VALUES(uuid_generate_v4(), $1::uuid, $2, NOW(), NULL, NULL, $3)
        RETURNING *;
      `;

      const { rows } = await waMeta.query(insertQuery, [integrationId, newFrom, blockResponse]);
      console.log('Inserted integration_work_around (META):', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in waMetaWorkAroundDatabase function:', error);
    throw new Error(`Error in waMetaWorkAroundDatabase function: ${error.message}`);
  }
}

async function wa360WorkAroundDatabase({ integrationId, newFrom, blockResponse }) {
  const checkQuery = `
    SELECT id FROM public.integration_work_around WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await wa360.query(checkQuery, [integrationId]);

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

      const { rows } = await wa360.query(updateQuery, [newFrom, blockResponse, integrationId]);
      console.log('Updated integration_work_around (360):', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.integration_work_around
        (id, integration_id, new_from, created_at, updated_at, description, block_response)
        VALUES(uuid_generate_v4(), $1::uuid, $2, NOW(), NULL, NULL, $3)
        RETURNING *;
      `;

      const { rows } = await wa360.query(insertQuery, [integrationId, newFrom, blockResponse]);
      console.log('Inserted integration_work_around (360):', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in wa360WorkAroundDatabase function:', error);
    throw new Error(`Error in wa360WorkAroundDatabase function: ${error.message}`);
  }
}

async function waInfoBipWorkAroundDatabase({ integrationId, newFrom, blockResponse }) {
  const checkQuery = `
    SELECT id FROM public.integration_work_around WHERE integration_id = $1::uuid;
  `;

  try {
    const { rows: existingRows } = await waInfoBip.query(checkQuery, [integrationId]);

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

      const { rows } = await waInfoBip.query(updateQuery, [newFrom, blockResponse, integrationId]);
      console.log('Updated integration_work_around (INFOBIP):', rows[0]);
      return rows[0];
    } else {
      const insertQuery = `
        INSERT INTO public.integration_work_around
        (id, integration_id, new_from, created_at, updated_at, description, block_response)
        VALUES(uuid_generate_v4(), $1::uuid, $2, NOW(), NULL, NULL, $3)
        RETURNING *;
      `;

      const { rows } = await waInfoBip.query(insertQuery, [integrationId, newFrom, blockResponse]);
      console.log('Inserted integration_work_around (INFOBIP):', rows[0]);
      return rows[0];
    }
  } catch (error) {
    console.error('Error in waInfoBipWorkAroundDatabase function:', error);
    throw new Error(`Error in waInfoBipWorkAroundDatabase function: ${error.message}`);
  }
}

// ==========================================================================
// template_config_pool
// Replicada nas 3 bases (META / 360 / INFOBIP). Apenas INSERT (sem checagem
// de existência prévia) — cada chamada cria uma nova linha na pool.
// Campos fixos seguem o padrão observado: type='TEXT', status='ACTIVE',
// use_meta_param=true, meta_params='[]', image_url=NULL, language='pt_BR',
// template_type='UTILITY'.
// ==========================================================================

async function waMetaTemplateConfigPoolDatabase({ integrationId, name, newFrom, poolId, buttonUrl, newIntegrationId }) {
  const insertQuery = `
    INSERT INTO public.template_config_pool
    (id, "type", name, status, use_meta_param, meta_params, image_url, "language", template_type, new_from, created_date, last_updated_date, pool_id, button_url, new_integration_id, integration_id)
    VALUES(uuid_generate_v4(), 'TEXT', $1, 'ACTIVE', true, '[]'::jsonb, NULL, 'pt_BR', 'UTILITY', $2, NOW(), NOW(), $3::uuid, $4, $5::uuid, $6::uuid)
    RETURNING *;
  `;

  try {
    const { rows } = await waMeta.query(insertQuery, [name, newFrom, poolId, buttonUrl, newIntegrationId, integrationId]);
    console.log('Inserted template_config_pool (META):', rows[0]);
    return rows[0];
  } catch (error) {
    console.error('Error in waMetaTemplateConfigPoolDatabase function:', error);
    throw new Error(`Error in waMetaTemplateConfigPoolDatabase function: ${error.message}`);
  }
}

async function wa360TemplateConfigPoolDatabase({ integrationId, name, newFrom, poolId, buttonUrl, newIntegrationId }) {
  const insertQuery = `
    INSERT INTO public.template_config_pool
    (id, "type", name, status, use_meta_param, meta_params, image_url, "language", template_type, new_from, created_date, last_updated_date, pool_id, button_url, new_integration_id, integration_id)
    VALUES(uuid_generate_v4(), 'TEXT', $1, 'ACTIVE', true, '[]'::jsonb, NULL, 'pt_BR', 'UTILITY', $2, NOW(), NOW(), $3::uuid, $4, $5::uuid, $6::uuid)
    RETURNING *;
  `;

  try {
    const { rows } = await wa360.query(insertQuery, [name, newFrom, poolId, buttonUrl, newIntegrationId, integrationId]);
    console.log('Inserted template_config_pool (360):', rows[0]);
    return rows[0];
  } catch (error) {
    console.error('Error in wa360TemplateConfigPoolDatabase function:', error);
    throw new Error(`Error in wa360TemplateConfigPoolDatabase function: ${error.message}`);
  }
}

async function waInfoBipTemplateConfigPoolDatabase({ integrationId, name, newFrom, poolId, buttonUrl, newIntegrationId }) {
  const insertQuery = `
    INSERT INTO public.template_config_pool
    (id, "type", name, status, use_meta_param, meta_params, image_url, "language", template_type, new_from, created_date, last_updated_date, pool_id, button_url, new_integration_id, integration_id)
    VALUES(uuid_generate_v4(), 'TEXT', $1, 'ACTIVE', true, '[]'::jsonb, NULL, 'pt_BR', 'UTILITY', $2, NOW(), NOW(), $3::uuid, $4, $5::uuid, $6::uuid)
    RETURNING *;
  `;

  try {
    const { rows } = await waInfoBip.query(insertQuery, [name, newFrom, poolId, buttonUrl, newIntegrationId, integrationId]);
    console.log('Inserted template_config_pool (INFOBIP):', rows[0]);
    return rows[0];
  } catch (error) {
    console.error('Error in waInfoBipTemplateConfigPoolDatabase function:', error);
    throw new Error(`Error in waInfoBipTemplateConfigPoolDatabase function: ${error.message}`);
  }
}

module.exports = {
  waMetaDatabase,
  wa360Database,
  lakeDatabase,
  waInfoBipDatabase,
  waMetaWorkAroundDatabase,
  wa360WorkAroundDatabase,
  waInfoBipWorkAroundDatabase,
  waMetaTemplateConfigPoolDatabase,
  wa360TemplateConfigPoolDatabase,
  waInfoBipTemplateConfigPoolDatabase
}