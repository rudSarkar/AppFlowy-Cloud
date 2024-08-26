CREATE TABLE IF NOT EXISTS af_policy (
  subject   TEXT NOT NULL,
  object    TEXT NOT NULL,
  action    TEXT NOT NULL,
  PRIMARY KEY (subject, object, action)
)
