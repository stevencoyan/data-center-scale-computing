CREATE TABLE IF NOT EXISTS dim_date (
    "Date_ID" VARCHAR(8) PRIMARY KEY,
    "ts" VARCHAR(255),
    "month" INT NOT NULL,
    "year" INT NOT NULL,
);

CREATE TABLE IF NOT EXISTS dim_outcomes (
    "Outcome_Type_ID" INT PRIMARY KEY,
    "Outcome Type" VARCHAR(255) NOT NULL,
);

CREATE TABLE IF NOT EXISTS dim_animals (
    "Animal_Key" INT PRIMARY KEY,
    "Name" VARCHAR(255),
    "Date of Birth" VARCHAR(255),
    'sex' VARCHAR(255),
    "Color" VARCHAR(255)
    "Animal Type" VARCHAR(255),
    "Breed" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fact_table (
    "Outcome_ID" SERIAL PRIMARY KEY,
    "Animal_Key" VARCHAR(255),
    "Date_ID" VARCHAR(255),
    "time" TIME NOT NULL,
    "Outcome_Type_ID" INT NOT NULL,
    "Outcome Subtype" VARCHAR(255),

    FOREIGN KEY ("Animal_Key") REFERENCES dim_animals("Animal_Key"),
    FOREIGN KEY ("Outcome_Type_ID") REFERENCES dim_outcomes("Outcome_Type_ID"),
    FOREIGN KEY ("Date_ID") REFERENCES dim_breed("Date_ID")
);

-- -- Create the dimension table for dates
-- CREATE TABLE IF NOT EXISTS dim_date (
--     "Date_ID" VARCHAR(8) PRIMARY KEY,
--     "ts" VARCHAR(255),
--     "month" INT,
--     "year" INT
-- );

-- -- Create the dimension table for outcomes
-- CREATE TABLE IF NOT EXISTS dim_outcomes (
--     "Outcome_Type_ID" INT PRIMARY KEY,
--     "Outcome_Type" VARCHAR(255) NOT NULL
-- );

-- -- Create the dimension table for animals
-- CREATE TABLE IF NOT EXISTS dim_animals (
--     "Animal_Key" PRIMARY KEY,
--     "Name" VARCHAR(255),
--     "Date of Birth" VARCHAR(255),
--     "sex" VARCHAR(255),
--     "Color" VARCHAR(255),
--     "Animal_Type" VARCHAR(255),
--     "Breed" VARCHAR(255)
-- );

-- -- Create the fact table for outcomes
-- CREATE TABLE IF NOT EXISTS fact_table (
--     "Outcome_ID" SERIAL PRIMARY KEY,
--     "Animal_Key" INT,
--     "Date_ID" VARCHAR(8),
--     "time" TIME NOT NULL,
--     "Outcome_Type_ID" INT NOT NULL,
--     "Outcome_Subtype" VARCHAR(255),
    
--     FOREIGN KEY ("Animal_Key") REFERENCES dim_animals("Animal_Key"),
--     FOREIGN KEY ("Outcome_Type_ID") REFERENCES dim_outcomes("Outcome_Type_ID"),
--     FOREIGN KEY ("Date_ID") REFERENCES dim_date("Date_ID")
-- );

