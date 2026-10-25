// Which schema string fields the console masks. Pure, so
// scripts/test_console_secret_field.py can run it under node.

const SECRET_FIELD = /password|passwd|secret|token|credential|(^|_)(api|access|private|signing|account)_?key$/i;

// A name ending in a locator suffix names where a value lives (a JSONPath, URL,
// topic, ARN, header), not the secret itself.
const LOCATOR_SUFFIX = /(_path|_url|_uri|_topic|_arn|_header|_field|_name|_endpoint)$/i;

function secretName(text) {
  if (!text) return false;
  if (LOCATOR_SUFFIX.test(text)) return false;
  return SECRET_FIELD.test(text);
}

export function isSecretField(name, title) {
  return secretName(name) || secretName(title);
}
