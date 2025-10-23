CREATE DATABASE aurelia_db;

CREATE TABLE concept_notes (
    id SERIAL PRIMARY KEY,
    concept VARCHAR(255) UNIQUE NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_concept ON concept_notes(concept);
CREATE INDEX idx_created_at ON concept_notes(created_at);