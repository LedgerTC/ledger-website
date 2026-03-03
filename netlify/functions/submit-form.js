// Ledger Trade & Capital - Contact Form Handler
// Netlify Function: /.netlify/functions/submit-form
//
// This function receives form submissions from the website contact page
// and processes them through HubSpot:
//   1. Look up or create Contact (de-dupe by email)
//   2. Look up or create Company (de-dupe by name)
//   3. Associate Contact <-> Company (only if Company is new)
//   4. Determine Ticket owner (Contact owner > Company owner > Russell)
//   5. Create Ticket with full submission summary
//   6. Associate Ticket to Contact and Company
//
// Environment variables required (set in Netlify dashboard):
//   HUBSPOT_TOKEN           - Private app token
//   HUBSPOT_PORTAL_ID       - HubSpot portal/account ID
//   HUBSPOT_PIPELINE_ID     - "Inbound Inquiries" pipeline ID
//   HUBSPOT_DEFAULT_OWNER_EMAIL - Fallback owner (russell@ledgertc.com)

const HUBSPOT_API = "https://api.hubapi.com";

// ─── Helper: make HubSpot API request ─────────────────────────────
async function hubspot(method, path, body) {
  const opts = {
    method,
    headers: {
      "Authorization": `Bearer ${process.env.HUBSPOT_TOKEN}`,
      "Content-Type": "application/json",
    },
  };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(`${HUBSPOT_API}${path}`, opts);
  const text = await res.text();

  let data;
  try { data = JSON.parse(text); } catch { data = text; }

  if (!res.ok) {
    console.error(`HubSpot API error [${res.status}] ${method} ${path}:`, data);
    return { error: true, status: res.status, data };
  }
  return data;
}

// ─── Step 1: Contact lookup / creation ────────────────────────────
async function findOrCreateContact(formData) {
  // Search by email
  const search = await hubspot("POST", "/crm/v3/objects/contacts/search", {
    filterGroups: [{
      filters: [{ propertyName: "email", operator: "EQ", value: formData.email }]
    }],
    properties: ["email", "firstname", "lastname", "hubspot_owner_id"],
  });

  if (search.total > 0) {
    const existing = search.results[0];
    console.log(`Contact found: ${existing.id} (${formData.email})`);
    return { contact: existing, isNew: false };
  }

  // Create new contact
  const created = await hubspot("POST", "/crm/v3/objects/contacts", {
    properties: {
      email: formData.email,
      firstname: formData.firstName,
      lastname: formData.lastName,
      phone: formData.phone,
      lifecyclestage: "lead",
    },
  });

  if (created.error) {
    throw new Error(`Failed to create contact: ${JSON.stringify(created.data)}`);
  }

  console.log(`Contact created: ${created.id} (${formData.email})`);
  return { contact: created, isNew: true };
}

// ─── Step 2: Company lookup / creation ────────────────────────────
async function findOrCreateCompany(companyName) {
  if (!companyName || companyName.trim() === "") {
    return { company: null, isNew: false };
  }

  // Search by name
  const search = await hubspot("POST", "/crm/v3/objects/companies/search", {
    filterGroups: [{
      filters: [{ propertyName: "name", operator: "EQ", value: companyName.trim() }]
    }],
    properties: ["name", "coverage"],
  });

  if (search.total > 0) {
    const existing = search.results[0];
    console.log(`Company found: ${existing.id} (${companyName})`);
    return { company: existing, isNew: false };
  }

  // Create new company
  const created = await hubspot("POST", "/crm/v3/objects/companies", {
    properties: {
      name: companyName.trim(),
    },
  });

  if (created.error) {
    throw new Error(`Failed to create company: ${JSON.stringify(created.data)}`);
  }

  console.log(`Company created: ${created.id} (${companyName})`);
  return { company: created, isNew: true };
}

// ─── Step 3: Associate Contact <-> Company ────────────────────────
async function associateContactToCompany(contactId, companyId) {
  const result = await hubspot(
    "PUT",
    `/crm/v4/objects/contacts/${contactId}/associations/companies/${companyId}`,
    [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 1 }]
  );
  if (result.error) {
    console.error("Failed to associate contact to company:", result.data);
  } else {
    console.log(`Associated contact ${contactId} <-> company ${companyId}`);
  }
}

// ─── Step 4: Determine Ticket owner ──────────────────────────────
async function determineTicketOwner(contactResult, companyResult) {
  // Priority 1: Existing contact's owner
  if (!contactResult.isNew && contactResult.contact.properties.hubspot_owner_id) {
    console.log(`Ticket owner from existing contact: ${contactResult.contact.properties.hubspot_owner_id}`);
    return contactResult.contact.properties.hubspot_owner_id;
  }

  // Priority 2: Existing company's owner (stored in "coverage" property)
  if (companyResult.company && !companyResult.isNew && companyResult.company.properties.coverage) {
    console.log(`Ticket owner from existing company coverage: ${companyResult.company.properties.coverage}`);
    return companyResult.company.properties.coverage;
  }

  // Priority 3: Default owner (Russell)
  const defaultEmail = process.env.HUBSPOT_DEFAULT_OWNER_EMAIL || "russell@ledgertc.com";
  const owners = await hubspot("GET", `/crm/v3/owners?email=${encodeURIComponent(defaultEmail)}`);

  if (owners.results && owners.results.length > 0) {
    console.log(`Ticket owner defaulting to ${defaultEmail}: ${owners.results[0].id}`);
    return owners.results[0].id;
  }

  console.warn("Could not find default owner. Ticket will be unassigned.");
  return null;
}

// ─── Step 5: Build ticket summary ────────────────────────────────
function buildTicketSummary(formData) {
  const lines = [
    "━━━ Contact Information ━━━",
    `Name: ${formData.firstName} ${formData.lastName}`,
    `Email: ${formData.email}`,
    `Phone: ${formData.phone}`,
  ];

  if (formData.company) {
    lines.push(`Company: ${formData.company}`);
  }

  lines.push("");
  lines.push("━━━ Inquiry Details ━━━");

  if (formData.loanType) lines.push(`Loan Type: ${formData.loanType}`);
  if (formData.loanAmount) lines.push(`Loan Amount: ${formData.loanAmount}`);
  if (formData.experience) lines.push(`Experience: ${formData.experience}`);
  if (formData.propertyAddress) lines.push(`Property Address: ${formData.propertyAddress}`);

  if (formData.projectOverview) {
    lines.push("");
    lines.push("━━━ Project Overview ━━━");
    lines.push(formData.projectOverview);
  }

  lines.push("");
  lines.push(`Submitted: ${new Date().toLocaleString("en-US", { timeZone: "America/New_York" })}`);
  lines.push(`Source: ${formData.pageUrl || "ledgertc.co/contact"}`);

  return lines.join("\n");
}

// ─── Step 6: Create Ticket ───────────────────────────────────────
async function createTicket(formData, ownerId, contactId, companyId) {
  const summary = buildTicketSummary(formData);
  const ticketName = `Inbound Inquiry — ${formData.firstName} ${formData.lastName}`;
  if (formData.company) {
    // Include company in ticket name for quick scanning
  }

  const properties = {
    subject: formData.company
      ? `${ticketName} (${formData.company})`
      : ticketName,
    content: summary,
    hs_pipeline: process.env.HUBSPOT_PIPELINE_ID,
    hs_pipeline_stage: "1",  // "New" stage — update this if your stage ID differs
    hs_ticket_priority: "MEDIUM",
  };

  if (ownerId) {
    properties.hubspot_owner_id = ownerId;
  }

  const ticket = await hubspot("POST", "/crm/v3/objects/tickets", { properties });

  if (ticket.error) {
    throw new Error(`Failed to create ticket: ${JSON.stringify(ticket.data)}`);
  }

  console.log(`Ticket created: ${ticket.id}`);

  // Associate ticket to contact
  if (contactId) {
    await hubspot(
      "PUT",
      `/crm/v4/objects/tickets/${ticket.id}/associations/contacts/${contactId}`,
      [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 16 }]
    );
    console.log(`Ticket ${ticket.id} associated to contact ${contactId}`);
  }

  // Associate ticket to company
  if (companyId) {
    await hubspot(
      "PUT",
      `/crm/v4/objects/tickets/${ticket.id}/associations/companies/${companyId}`,
      [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 26 }]
    );
    console.log(`Ticket ${ticket.id} associated to company ${companyId}`);
  }

  return ticket;
}

// ─── Main handler ────────────────────────────────────────────────
exports.handler = async function (event) {
  // CORS headers
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };

  // Handle preflight
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers, body: "" };
  }

  // Only accept POST
  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: "Method not allowed" }),
    };
  }

  try {
    // Parse form data
    const formData = JSON.parse(event.body);

    // Validate required fields
    const required = ["firstName", "lastName", "email", "phone"];
    const missing = required.filter((f) => !formData[f] || formData[f].trim() === "");
    if (missing.length > 0) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: `Missing required fields: ${missing.join(", ")}` }),
      };
    }

    // Basic email validation
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email)) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: "Invalid email address" }),
      };
    }

    console.log(`Processing submission from ${formData.email}`);

    // Step 1: Contact
    const contactResult = await findOrCreateContact(formData);
    const contactId = contactResult.contact.id;

    // Step 2: Company
    const companyResult = await findOrCreateCompany(formData.company);
    const companyId = companyResult.company ? companyResult.company.id : null;

    // Step 3: Associate Contact <-> Company (only if company is NEW)
    if (companyId && companyResult.isNew && contactId) {
      await associateContactToCompany(contactId, companyId);
    }

    // Step 4: Determine ticket owner
    const ownerId = await determineTicketOwner(contactResult, companyResult);

    // Step 5 & 6: Create ticket with summary and associations
    const ticket = await createTicket(formData, ownerId, contactId, companyId);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: "Your application has been submitted. Our team will follow up within one business day.",
        ticketId: ticket.id,
      }),
    };

  } catch (err) {
    console.error("Error processing form submission:", err);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: "Something went wrong. Please try again or contact us directly.",
      }),
    };
  }
};
