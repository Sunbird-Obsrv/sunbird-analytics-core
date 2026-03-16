CREATE KEYSPACE IF NOT EXISTS local_platform_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_platform_db.experiment_definition (
    exp_id text,
    created_by text,
    created_on timestamp,
    criteria text,
    exp_data text,
    exp_description text,
    exp_name text,
    stats map<text, bigint>,
    status text,
    status_message text,
    updated_by text,
    updated_on timestamp,
    PRIMARY KEY(exp_id)
);

INSERT INTO local_platform_db.experiment_definition(exp_id, exp_name, criteria, exp_data, status) VALUES ('U1234', 'USER_ORG', '{"type":"user","filters":{"organisations.orgName":["sunbird"]}}', '{"startDate":"2019-08-06","endDate":"2019-08-09","key":"/org/profile","client":"portal"}', 'SUBMITTED');
