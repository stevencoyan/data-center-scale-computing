-- 1. How many animals of each type have outcomes?

SELECT "Animal Type" AS "Animal", COUNT(*) AS "Animal Count"
FROM dim_breed
Group BY "Animal Type";

-- 2. How many animals have more than 1 outcome?

SELECT COUNT(DISTINCT ft."Animal_Key") AS "Animals with More than 1 Outcome"
FROM fact_table ft
GROUP BY ft."Animal_Key"
HAVING COUNT(DISTINCT ft."Outcome_ID") > 1;

-- 3. What are the top 5 months for outcomes? 

SELECT "month", COUNT(*) AS "Outcome Count"
FROM dim_outcomes
GROUP BY "month"
ORDER BY "Outcome Count" DESC
LIMIT 5;

-- 4. What is the total number of kittens, adults, and seniors, whose outcome is "Adopted"?
-- Conversely, among all the cats who were "Adopted", what is the total number of kittens, adults, and seniors?
-- Note: Each Question will output the same answer

-- Add a new column to store the age of the animals in days
ALTER TABLE dim_animals
ADD COLUMN "Age in Days" INT;

-- Update the "Age in Days" column with the calculated values
UPDATE dim_animals
SET "Age in Days" = (CURRENT_DATE - TO_DATE("Date of Birth", 'MM/DD/YYYY'))::INT;


-- Query Process: Combined 3 tables to get Animal Type, Outcome Type and Age in Days with
-- correct filters

--SELECT
--    db."Animal Type",
--    dc."Outcome Type",
--    da."Age in Days"
--FROM
--    dim_breed db
--JOIN
--    dim_outcomes dc
--ON
--    db."Breed_ID" = dc."Outcome_ID"
--JOIN
--    dim_animals da
--ON
--    da."Animal_Key" = db."Breed_ID"
--where 
--	db."Animal Type" = 'Cat'
--and 
--	dc."Outcome Type" = 'Adoption';


-- Change Animal Type cat name to fit each cat type

--SELECT
--    CASE
--        WHEN da."Age in Days" < 365 THEN 'Kitten'
--        WHEN da."Age in Days" BETWEEN 365 AND 3650 THEN 'Adult'
--        WHEN da."Age in Days" > 3650 THEN 'Senior cat'
--    END AS "Animal Type",
--    dc."Outcome Type",
--    da."Age in Days"
--FROM
--    dim_breed db
--JOIN
--    dim_outcomes dc
--ON
--    db."Breed_ID" = dc."Outcome_ID"
--JOIN
--    dim_animals da
--ON
--    da."Animal_Key" = db."Breed_ID"
--WHERE
--    db."Animal Type" = 'Cat'
--    AND dc."Outcome Type" = 'Adoption';


-- Final Query

SELECT
    "Animal Type",
    COUNT(*) AS "Count"
FROM (
    SELECT
        case -- set cat types
            WHEN da."Age in Days" < 365 THEN 'Kitten'
            WHEN da."Age in Days" BETWEEN 365 AND 3650 THEN 'Adult'
            WHEN da."Age in Days" > 3650 THEN 'Senior cat'
        END AS "Animal Type",
        dc."Outcome Type",
        da."Age in Days"
    from -- joins 
        dim_breed db
    JOIN
        dim_outcomes dc
    ON
        db."Breed_ID" = dc."Outcome_ID"
    JOIN
        dim_animals da
    ON
        da."Animal_Key" = db."Breed_ID"
    where -- filters
        db."Animal Type" = 'Cat'
    AND 
    	dc."Outcome Type" = 'Adoption')
    GROUP BY
    	"Animal Type";
   
 
   
   
 -- 5. For each date, what is the cumulative total of outcomes up to and including this date?

WITH date_series AS (
  SELECT
    generate_series( -- combine month and year and set min and max, also convert to date type
      date_trunc('month', MIN(TO_DATE(CONCAT(year, month, '01'), 'YYYYMonDD')::date)),
      date_trunc('month', MAX(TO_DATE(CONCAT(year, month, '01'), 'YYYYMonDD')::date)),
      interval '1 month'
    ) AS time
  FROM dim_outcomes
)
SELECT
  TO_CHAR(ds.time, 'Month YYYY') AS Date,
  SUM(COUNT(*)) OVER (ORDER BY ds.time) AS Outcomes
FROM date_series ds
JOIN dim_outcomes ON ds.time = date_trunc('month', TO_DATE(CONCAT(dim_outcomes.year, dim_outcomes.month, '01'), 'YYYYMonDD')::date)
GROUP BY ds.time;











