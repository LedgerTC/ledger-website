const fs = require('fs');
const path = require('path');

exports.handler = async function(event, context) {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers, body: 'Method not allowed' };
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch(e) {
    return { statusCode: 400, headers, body: 'Bad request' };
  }

  const { orgId, docId, password } = body;

  if (!orgId || !docId || !password) {
    return { statusCode: 400, headers, body: 'Missing parameters' };
  }

  // Admin auth check
  if (orgId === 'admin' && docId === 'admin') {
    const adminPassword = process.env.ADMIN_PORTAL_PASSWORD;
    if (!adminPassword || password !== adminPassword) {
      return { statusCode: 401, headers, body: 'Unauthorized' };
    }
    return { statusCode: 200, headers, body: 'ok' };
  }

  // Validate orgId and docId format (numeric only, max 4 chars)
  if (!/^\d{1,4}$/.test(orgId) || !/^\d{1,4}$/.test(docId)) {
    return { statusCode: 403, headers, body: 'Forbidden' };
  }

  // Check password against env var
  const envKey = `DOC_${orgId.padStart(4, '0')}_${docId.padStart(4, '0')}_PASSWORD`;
  const expectedPassword = process.env[envKey];

  if (!expectedPassword || password !== expectedPassword) {
    return { statusCode: 401, headers, body: 'Unauthorized' };
  }

  // Read document file
  const filePath = path.join(__dirname, 'documents', `${orgId.padStart(4, '0')}-${docId.padStart(4, '0')}.html`);

  let fileContent;
  try {
    fileContent = fs.readFileSync(filePath, 'utf8');
  } catch(e) {
    return { statusCode: 403, headers, body: 'Forbidden' };
  }

  return {
    statusCode: 200,
    headers: { ...headers, 'Content-Type': 'text/html' },
    body: fileContent
  };
};
