CREATE TABLE flyway_cert (
    id INT NOT NULL,
    label TEXT,
    PRIMARY KEY (id)
);

INSERT INTO flyway_cert (id, label) VALUES (1, 'alpha');
