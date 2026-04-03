// Ledger Trade & Capital - Loan Application Handler
// Netlify Function: /.netlify/functions/submit-loan-app
//
// Receives the loan application form data as JSON, validates it,
// creates a DocuSeal submission with all fields pre-filled and readonly,
// and returns the embed_src URL so the browser can show the signature step.
//
// Environment variables required (set in Netlify dashboard):
//   DOCUSEAL_API_KEY      - DocuSeal API token
//   TURNSTILE_SECRET_KEY  - Cloudflare Turnstile secret

const DOCUSEAL_API = "https://docuseal.com/api";
const TEMPLATE_ID = 3326505;

// ─── Field mapping: HTML form name → DocuSeal template field name ────
// Text/phone/date fields map value directly.
// Radio fields map "Yes"/"No" string.
// Checkbox fields map true/false boolean.
const FIELD_MAP = {
  // Entity & Property
  entity_name:          "Entity Name",
  ein:                  "EIN",
  property_address:     "Property Address",
  cross_collateralized: "Cross-Collateralized",

  // Guarantor info
  guarantor_name:       "Guarantor Name",
  guarantor_ownership:  "Guarantor Ownership %",
  guarantor_ssn:        "Guarantor SSN",
  guarantor_dob:        "Guarantor DOB",
  guarantor_credit:     "Guarantor Credit Score",
  guarantor_phone:      "Guarantor Phone",
  guarantor_email:      "Guarantor Email",
  guarantor_address:    "Guarantor Address",

  // Co-Guarantor gate
  has_co_guarantor:     "Has Co-Guarantor",

  // Co-Guarantor info
  co_guarantor_name:       "Co-Guarantor Name",
  co_guarantor_ownership:  "Co-Guarantor Ownership %",
  co_guarantor_ssn:        "Co-Guarantor SSN",
  co_guarantor_dob:        "Co-Guarantor DOB",
  co_guarantor_credit:     "Co-Guarantor Credit Score",
  co_guarantor_phone:      "Co-Guarantor Phone",
  co_guarantor_email:      "Co-Guarantor Email",
  co_guarantor_address:    "Co-Guarantor Address",

  // Guarantor declarations
  g_us_citizen:         "G - US Citizen",
  g_permanent_resident: "G - Permanent Resident",
  g_confirm_all_no:     "G - Confirm All No",
  g_bankrupt_5yr:       "G - Bankrupt 5yr",
  g_foreclosed_4yr:     "G - Foreclosed 4yr",
  g_lawsuit:            "G - Lawsuit",
  g_loan_default:       "G - Loan Default",
  g_federal_debt:       "G - Federal Debt",
  g_felony:             "G - Felony",
  g_judgments:          "G - Judgments",
  g_occupy_property:    "G - Occupy Property",
  g_ever_occupied:      "G - Ever Occupied",
  g_under_construction: "G - Under Construction",
  g_hazards:            "G - Hazards",
  g_seller_relationship:"G - Seller Relationship",
  g_wholesaler:         "G - Wholesaler",
  g_down_payment:       "G - Down Payment Borrowed",
  g_short_sale:         "G - Short Sale",

  // Co-Guarantor declarations
  cg_us_citizen:         "CG - US Citizen",
  cg_permanent_resident: "CG - Permanent Resident",
  cg_confirm_all_no:     "CG - Confirm All No",
  cg_bankrupt_5yr:       "CG - Bankrupt 5yr",
  cg_foreclosed_4yr:     "CG - Foreclosed 4yr",
  cg_lawsuit:            "CG - Lawsuit",
  cg_loan_default:       "CG - Loan Default",
  cg_federal_debt:       "CG - Federal Debt",
  cg_felony:             "CG - Felony",
  cg_judgments:          "CG - Judgments",
  cg_occupy_property:    "CG - Occupy Property",
  cg_ever_occupied:      "CG - Ever Occupied",
  cg_under_construction: "CG - Under Construction",
  cg_hazards:            "CG - Hazards",
  cg_seller_relationship:"CG - Seller Relationship",
  cg_wholesaler:         "CG - Wholesaler",
  cg_down_payment:       "CG - Down Payment Borrowed",
  cg_short_sale:         "CG - Short Sale",

  // Title & Insurance
  title_first_integrity: "Title - First Integrity",
  title_own_choice:      "Title - Own Choice",
  title_company_name:    "Title Company Name",
  title_company_contact: "Title Company Contact",
  title_company_phone:   "Title Company Phone",
  title_company_email:   "Title Company Email",
  title_company_address: "Title Company Address",
  insurance_company:     "Insurance Company Name",
  insurance_contact:     "Insurance Contact",
  insurance_email:       "Insurance Email",
};

// Fields that are checkboxes (boolean values)
const CHECKBOX_FIELDS = new Set([
  "g_confirm_all_no", "cg_confirm_all_no",
  "title_first_integrity", "title_own_choice",
]);

// Fields that should NOT be made readonly (signature fields handled by DocuSeal)
const SIGNATURE_FIELDS = new Set([
  "Guarantor Signature", "Guarantor Sign Date",
  "Co-Guarantor Signature", "Co-Guarantor Sign Date",
]);

// Required fields
const REQUIRED = [
  "entity_name", "property_address",
  "guarantor_name", "guarantor_ssn", "guarantor_dob",
  "guarantor_phone", "guarantor_email", "guarantor_address",
];

// ─── Main handler ────────────────────────────────────────────────
exports.handler = async function (event) {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers, body: "" };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };
  }

  try {
    const data = JSON.parse(event.body || "{}");

    // ── Honeypot check ──────────────────────────────────────────
    if (data.website) {
      console.log("Honeypot triggered");
      // Return fake success so bots think it worked
      return { statusCode: 200, headers, body: JSON.stringify({ success: true, embed_src: "" }) };
    }

    // ── Timing check (min 3 seconds) ────────────────────────────
    const loadedAt = parseInt(data.form_loaded_at || "0", 10);
    if (loadedAt && Date.now() - loadedAt < 3000) {
      console.log("Form submitted too fast");
      return { statusCode: 200, headers, body: JSON.stringify({ success: true, embed_src: "" }) };
    }

    // ── Turnstile verification ──────────────────────────────────
    const turnstileToken = data["cf-turnstile-response"] || "";
    if (!turnstileToken) {
      return {
        statusCode: 403, headers,
        body: JSON.stringify({ success: false, error: "CAPTCHA verification required. Please try again." }),
      };
    }

    const turnstileRes = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        secret: process.env.TURNSTILE_SECRET_KEY,
        response: turnstileToken,
        remoteip: (event.headers["x-forwarded-for"] || "").split(",")[0].trim(),
      }),
    });
    const turnstileData = await turnstileRes.json();
    if (!turnstileData.success) {
      console.log("Turnstile failed:", turnstileData);
      return {
        statusCode: 403, headers,
        body: JSON.stringify({ success: false, error: "CAPTCHA verification failed. Please refresh and try again." }),
      };
    }

    // ── Validate required fields ────────────────────────────────
    const missing = REQUIRED.filter((f) => !data[f] || !data[f].toString().trim());
    if (missing.length > 0) {
      return {
        statusCode: 400, headers,
        body: JSON.stringify({ success: false, error: `Missing required fields: ${missing.join(", ")}` }),
      };
    }

    // ── Build DocuSeal values object ────────────────────────────
    const values = {};
    for (const [htmlName, docusealName] of Object.entries(FIELD_MAP)) {
      const raw = data[htmlName];
      if (raw === undefined || raw === null || raw === "") continue;

      if (CHECKBOX_FIELDS.has(htmlName)) {
        values[docusealName] = raw === true || raw === "true" || raw === "on";
      } else {
        values[docusealName] = raw;
      }
    }

    // ── Build readonly fields list ──────────────────────────────
    const readonlyFieldNames = Object.values(FIELD_MAP);

    // ── Create DocuSeal submission ──────────────────────────────
    const submissionPayload = {
      template_id: TEMPLATE_ID,
      send_email: false,
      submitters: [
        {
          role: "First Party",
          email: data.guarantor_email || "applicant@ledgertc.com",
          name: data.guarantor_name || "Applicant",
          values: values,
          readonly_fields: readonlyFieldNames,
        },
      ],
    };

    console.log("Creating DocuSeal submission for:", data.guarantor_name, data.guarantor_email);

    const dsRes = await fetch(`${DOCUSEAL_API}/submissions`, {
      method: "POST",
      headers: {
        "X-Auth-Token": process.env.DOCUSEAL_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(submissionPayload),
    });

    const dsData = await dsRes.json();

    if (!dsRes.ok) {
      console.error("DocuSeal API error:", dsRes.status, JSON.stringify(dsData));
      return {
        statusCode: 502, headers,
        body: JSON.stringify({ success: false, error: "Failed to create signing session. Please try again." }),
      };
    }

    // Response is an array of submitter objects
    const submitter = Array.isArray(dsData) ? dsData[0] : dsData;
    const embedSrc = submitter.embed_src || "";

    if (!embedSrc) {
      console.error("No embed_src in response:", JSON.stringify(dsData));
      return {
        statusCode: 502, headers,
        body: JSON.stringify({ success: false, error: "Signing session created but no signing URL returned." }),
      };
    }

    console.log("DocuSeal submission created. Submission ID:", submitter.submission_id || submitter.id);

    return {
      statusCode: 200, headers,
      body: JSON.stringify({
        success: true,
        embed_src: embedSrc,
        submission_id: submitter.submission_id || submitter.id,
      }),
    };

  } catch (err) {
    console.error("Unhandled error:", err);
    return {
      statusCode: 500, headers,
      body: JSON.stringify({ success: false, error: "An unexpected error occurred. Please try again." }),
    };
  }
};
