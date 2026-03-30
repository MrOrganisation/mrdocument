// Bootstrap Directus configuration for mrdocument.
// Registers the documents_v2 collection, configures fields,
// and creates a "MrDocument User" role with per-user read + edit access.
// Idempotent — safe to run on every container start.

const DIRECTUS_URL = process.env.DIRECTUS_URL ?? 'http://directus:8055';
const ADMIN_EMAIL = process.env.DIRECTUS_ADMIN_EMAIL;
const ADMIN_PASSWORD = process.env.DIRECTUS_ADMIN_PASSWORD;

const log = (msg) => console.log(`[directus-init] ${msg}`);

let token;

async function api(path, { method = 'GET', body, allowNotFound = false } = {}) {
  const opts = {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
  };
  if (body) opts.body = JSON.stringify(body);

  const r = await fetch(`${DIRECTUS_URL}${path}`, opts);
  if (r.status === 404 && allowNotFound) return null;
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`${method} ${path} → ${r.status}: ${text}`);
  }
  const ct = r.headers.get('content-type');
  if (ct?.includes('application/json')) return (await r.json()).data;
  return null;
}

// ---- Wait & authenticate ----------------------------------------------------

async function waitForDirectus() {
  log('Waiting for Directus...');
  for (let i = 0; i < 90; i++) {
    try {
      const r = await fetch(`${DIRECTUS_URL}/server/health`);
      if (r.ok) { log('Directus is ready.'); return; }
    } catch {}
    await new Promise((r) => setTimeout(r, 2000));
  }
  throw new Error('Directus did not become ready within 3 minutes');
}

async function login() {
  const r = await fetch(`${DIRECTUS_URL}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: ADMIN_EMAIL, password: ADMIN_PASSWORD }),
  });
  if (!r.ok) throw new Error(`Login failed: ${r.status} ${await r.text()}`);
  token = (await r.json()).data.access_token;
  log('Authenticated.');
}

// ---- Collection -------------------------------------------------------------

async function registerCollection() {
  const existing = await api('/collections/documents_v2', { allowNotFound: true });
  if (existing) {
    log('Collection documents_v2 already registered.');
    return;
  }

  log('Registering documents_v2 collection...');
  await api('/collections', {
    method: 'POST',
    body: {
      collection: 'documents_v2',
      schema: { schema: 'mrdocument', name: 'documents_v2' },
      meta: {
        icon: 'description',
        note: 'MrDocument processed documents',
        display_template: '{{original_filename}}',
        hidden: false,
        singleton: false,
        accountability: 'all',
      },
    },
  });
  log('Collection registered.');
}

// ---- Fields -----------------------------------------------------------------

const FIELD_META = [
  // Primary — visible, top of detail page
  { field: 'id',                meta: { sort: 1,  width: 'half', hidden: false, readonly: true } },
  { field: 'original_filename', meta: { sort: 2,  width: 'half', hidden: false, readonly: true } },
  { field: 'assigned_filename', meta: { sort: 3,  width: 'half', hidden: false, readonly: true } },
  { field: 'output_filename',   meta: { sort: 4,  width: 'half', hidden: false, readonly: true } },
  { field: 'state',             meta: { sort: 5,  width: 'half', hidden: false, readonly: true,
    interface: 'select-dropdown',
    options: { choices: [
      { text: 'New',              value: 'is_new' },
      { text: 'Needs Processing', value: 'needs_processing' },
      { text: 'Missing',          value: 'is_missing' },
      { text: 'Error',            value: 'has_error' },
      { text: 'Needs Deletion',   value: 'needs_deletion' },
      { text: 'Deleted',          value: 'is_deleted' },
      { text: 'Complete',         value: 'is_complete' },
    ] },
  } },
  { field: 'context',           meta: { sort: 6,  width: 'half', hidden: false,
    interface: 'select-dropdown',
    options: { choices: [], allowOther: false },
    note: 'Document category. Changing this reclassifies the document. Choices are populated by the watcher.',
  } },
  { field: 'username',          meta: { sort: 7,  width: 'half', hidden: false, readonly: true } },

  // Dates — readonly system timestamps
  { field: 'date_added',        meta: { sort: 10, width: 'half', hidden: false, readonly: true,
    interface: 'datetime', display: 'datetime',
    options: { includeSeconds: true },
  } },
  { field: 'created_at',        meta: { sort: 11, width: 'half', hidden: false, readonly: true,
    interface: 'datetime', display: 'datetime',
    options: { includeSeconds: true },
  } },
  { field: 'updated_at',        meta: { sort: 12, width: 'half', hidden: false, readonly: true,
    interface: 'datetime', display: 'datetime',
    options: { includeSeconds: true },
  } },

  // Description, summary, language — AI-generated, readonly
  { field: 'description',      meta: { sort: 13, width: 'full', hidden: false, readonly: true,
    note: 'AI-generated short description of the document.',
  } },
  { field: 'summary',          meta: { sort: 14, width: 'full', hidden: false, readonly: true,
    interface: 'input-multiline',
    note: 'AI-generated detailed summary of the document.',
  } },
  { field: 'language',         meta: { sort: 14, width: 'half', hidden: false, readonly: true,
    note: 'ISO 639-1 language code detected by AI (e.g. de, en).',
  } },
  { field: 'content',          meta: { sort: 23, width: 'full', hidden: true, readonly: true,
    interface: 'input-multiline',
    note: 'Full text content of the processed document.',
  } },

  // Tags — user-editable list of strings
  { field: 'tags',              meta: { sort: 15, width: 'full', hidden: false,
    interface: 'tags', options: { iconRight: 'label' },
    note: 'User-defined tags. Used for smart folder conditions.',
  } },

  // JSON — metadata is editable, paths are readonly
  { field: 'metadata',          meta: { sort: 20, width: 'full', hidden: false,
    interface: 'input-code', options: { language: 'JSON' },
    note: 'Document fields extracted by AI (e.g. date, type, sender). Edit to correct classifications.',
  } },
  { field: 'source_paths',      meta: { sort: 21, width: 'full', hidden: false, readonly: true,
    interface: 'input-code', options: { language: 'JSON' } } },
  { field: 'current_paths',     meta: { sort: 22, width: 'full', hidden: false, readonly: true,
    interface: 'input-code', options: { language: 'JSON' } } },

  // Technical — hidden from default view
  { field: 'source_hash',          meta: { sort: 90, hidden: true, readonly: true } },
  { field: 'hash',                 meta: { sort: 91, hidden: true, readonly: true } },
  { field: 'content_hash',         meta: { sort: 92, hidden: true, readonly: true } },
  { field: 'source_content_hash',  meta: { sort: 93, hidden: true, readonly: true } },
  { field: 'missing_source_paths', meta: { sort: 94, hidden: true, readonly: true } },
  { field: 'missing_current_paths',meta: { sort: 95, hidden: true, readonly: true } },
  { field: 'target_path',          meta: { sort: 96, hidden: true, readonly: true } },
  { field: 'source_reference',     meta: { sort: 97, hidden: true, readonly: true } },
  { field: 'current_reference',    meta: { sort: 98, hidden: true, readonly: true } },
  { field: 'duplicate_sources',    meta: { sort: 99, hidden: true, readonly: true } },
  { field: 'deleted_paths',        meta: { sort: 100, hidden: true, readonly: true } },
];

async function configureFields() {
  log('Configuring fields...');
  for (const { field, meta } of FIELD_META) {
    try {
      await api(`/fields/documents_v2/${field}`, { method: 'PATCH', body: { meta } });
    } catch (e) {
      // Field might not exist yet (added by a migration), skip gracefully
      log(`  skip ${field}: ${e.message.slice(0, 80)}`);
    }
  }
  log('Fields configured.');
}

// ---- Role & permissions -----------------------------------------------------
// Directus v11+ uses policies as the permission container.
//   Role  <-- access -->  Policy  <-- has -->  Permission
// Users are assigned to a role; the role's linked policies determine access.

async function ensureRole() {
  const roles = await api('/roles');
  let role = roles.find((r) => r.name === 'MrDocument User');
  if (role) {
    log(`Role exists: ${role.id}`);
    return role.id;
  }
  log('Creating role...');
  role = await api('/roles', {
    method: 'POST',
    body: { name: 'MrDocument User', icon: 'person' },
  });
  log(`Role created: ${role.id}`);
  return role.id;
}

async function ensurePolicy(roleId) {
  const policies = await api('/policies');
  let policy = policies.find((p) => p.name === 'MrDocument Read Own');
  if (policy) {
    log(`Policy exists: ${policy.id}`);
    return policy.id;
  }
  log('Creating policy...');
  policy = await api('/policies', {
    method: 'POST',
    body: {
      name: 'MrDocument Read Own',
      icon: 'verified_user',
      admin_access: false,
      app_access: true,
    },
  });
  log(`Policy created: ${policy.id}`);

  // Link policy to role
  log('Linking policy to role...');
  await api('/access', {
    method: 'POST',
    body: { role: roleId, policy: policy.id },
  });
  log('Policy linked.');
  return policy.id;
}

// Fields that users may edit via Directus.
const USER_EDITABLE_FIELDS = [
  'metadata',
  'tags',
  'context',
];

async function ensurePermissions(policyId) {
  const perms = await api(`/permissions?filter[policy][_eq]=${policyId}`);

  // --- read on documents_v2 ---
  const hasRead = perms.some(
    (p) => p.collection === 'documents_v2' && p.action === 'read',
  );
  if (hasRead) {
    log('Read permission already exists.');
  } else {
    log('Creating read permission (filtered by username = current user)...');
    await api('/permissions', {
      method: 'POST',
      body: {
        policy: policyId,
        collection: 'documents_v2',
        action: 'read',
        permissions: {
          username: { _eq: '$CURRENT_USER.external_identifier' },
        },
        fields: ['*'],
      },
    });
    log('Read permission created.');
  }

  // --- update on documents_v2 (restricted fields) ---
  const hasUpdate = perms.some(
    (p) => p.collection === 'documents_v2' && p.action === 'update',
  );
  if (hasUpdate) {
    log('Update permission already exists.');
  } else {
    log('Creating update permission (restricted fields)...');
    await api('/permissions', {
      method: 'POST',
      body: {
        policy: policyId,
        collection: 'documents_v2',
        action: 'update',
        permissions: {
          username: { _eq: '$CURRENT_USER.external_identifier' },
        },
        fields: USER_EDITABLE_FIELDS,
      },
    });
    log('Update permission created.');
  }

  // --- directus_files read (app shell) ---
  const hasFiles = perms.some(
    (p) => p.collection === 'directus_files' && p.action === 'read',
  );
  if (!hasFiles) {
    await api('/permissions', {
      method: 'POST',
      body: {
        policy: policyId,
        collection: 'directus_files',
        action: 'read',
        permissions: {},
        fields: ['*'],
      },
    });
  }
}

// ---- Main -------------------------------------------------------------------

async function main() {
  await waitForDirectus();
  await login();
  await registerCollection();
  await configureFields();

  const roleId = await ensureRole();
  const policyId = await ensurePolicy(roleId);
  await ensurePermissions(policyId);

  log('Bootstrap complete.');
  log(`Role ID: ${roleId}`);
  log('Users are created automatically by mrdocument-watcher as it discovers user directories.');
}

main().catch((e) => {
  console.error(`[directus-init] FATAL: ${e.message}`);
  process.exit(1);
});
