CREATE TABLE IF NOT EXISTS dim_animals (
    "Animal_Key" INT PRIMARY KEY,
    "Name" VARCHAR(255),
    "Date of Birth" VARCHAR(255),
    "sex" VARCHAR(255),
    "Color" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_outcomes (
    "Outcome_ID" INT PRIMARY KEY,
    "Outcome Type" VARCHAR(255),
    "Outcome Subtype" VARCHAR(255),
    "Age upon Outcome" VARCHAR(255),
    "month" VARCHAR(255),
    "year" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_breed (
    "Breed_ID" INT PRIMARY KEY,
    "Animal Type" VARCHAR(255),
    "Breed" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fact_table (
    "Animal ID" VARCHAR(255),
    "DateTime" VARCHAR(255),
    "Animal_Key" INT,
    "Outcome_ID" INT,
    "Breed_ID" INT,

    FOREIGN KEY ("Animal_Key") REFERENCES dim_animals("Animal_Key"),
    FOREIGN KEY ("Outcome_ID") REFERENCES dim_outcomes("Outcome_ID"),
    FOREIGN KEY ("Breed_ID") REFERENCES dim_breed("Breed_ID")
);