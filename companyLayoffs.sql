-- PART 1: Data Cleaning

-- Steps:
-- 1. Remove Duplicates
-- 2. Standardize Data to find errors in data to fix (Ex: deal with spelling errors)
-- 3. Handle Null values
-- 4. Removes any columns that aren't necessary

-- PART 1: Remove Duplicates
-- Creates new table called layoffs_staging to keep raw data intact from manipulation
CREATE TABLE layoffs_staging
Like layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Checks to see which entries have duplicate entries
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Creates a new table to deal with these duplicate entries more easily
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Finds the entries with duplicate entries using the newly created row_num column
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Removes the duplicate entries from the new table
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deletes the duplicate entries from the new table
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- PART 2: Standardizing Data
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Removes whitespaces to make it look better
UPDATE layoffs_staging2
SET company = TRIM(company);

-- No longer need this row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Checks industry column
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Theres a 'Crypto', 'Crypto Currency' and a 'CryptoCurrency' industry, need to fix this
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Updates 'Crypto Currency' and 'CryptoCurrency' into just 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Checks location column
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- Checks country column
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- There exists a 'United States' and a 'United States.' so need to remove '.'
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

-- Checks to see if removes '.' from 'United States'
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- It worked so this updates the table to remove it
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Date column is in text column, need to convert to date column type
SELECT `date`
FROM layoffs_staging2
ORDER BY 1; 

-- Converts from text to date format to see if works
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Updates table entries in column to be in date format, but still not classified as date column
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Converts column to be defined at date instead of text
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Deal with Null and '' values

-- Sees null values for total_laid_off but some may need to be kept
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

-- This is the useless data may need to be removed since both these columns are null
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Checks industry column for null or '' values from earlier I remember seeing
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Populate missing data with information I know should be there
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Changes '' values to null because the intial query didn't work until I did this
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Sucessfully populated missing data from entries that were missing it like 'Airbnb'
-- where one of its entries was missing its industry column and and other was labelled
-- as 'Travel' so I populated the missing entry with it as well
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Checks to see why this entry wasn't changed but this is fine since
-- there's not enough information to populate this entry with certainty
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2;

-- 4. Remove unecessary rows and columns
-- Checks to see if there are entires where both of these columns are null
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deletes these entries because I believe this are useless and don't tell anything useful
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Final cleaned data displayed
SELECT *
FROM layoffs_staging2;

------------------------------------------------------

-- PART 2: Exploratory Data Analysis

-- Checks if there exists companies that laid off all their employees
-- and which most amount of employees laid off by a company total
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Shows which companies laid off all their employees
-- These are essentially companies that went out of business during COVID
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Shows which companiy laid off the most from 2020 to 2023
-- Big companies like Amazon, Google, Meta, Salesforce and Microsoft had the most layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Checks the beginning and end date for this dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Showed which industries had the most layoffs
-- The consumer and retail industries had the most layoffs
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Showed which countries had the most layoffs from 2020 to 2023
-- The United States had the most by far according to this dataset
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Shows the total amount of layoffs each year from all companies together
-- The year with most layoffs was 2022 most likely due to COVID
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Shows which stage where companies at that laid off the most
-- Companies at POST-IPO stage had the most layoffs
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
WHERE stage IS NOT NULL
GROUP BY stage
ORDER BY 2 DESC;

-- Shows the total amount of layoffs from all companies together each month of each year
-- From 2020-04 to 2020-05 and from 2022-05 to 2023-02 were the periods with the most lay offs
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1;

-- Shows that January, 2023 was the month and year with the most lay offs total
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 2 DESC;

-- Shows the total amount of layoffs from 2020 to 2023 as a rolling sum
-- There was 383159 employees laid off from 2020-03 to 2023-03 according to this dataset
WITH roll_total_cte AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS roll_total
FROM roll_total_cte;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
ORDER BY company;

-- Shows which companies laid off the most in this period
-- Google, Meta, Amazon and Microsoft laid off the most in years 2022 and 2023
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
ORDER BY 3 DESC;

-- Ranks the companies with the most layoffs that year in order by ranking
WITH company_year_cte (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
)
SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year_cte
WHERE years IS NOT NULL
ORDER BY ranking;

-- Used to check if the ranking was correct
SELECT company, SUM(total_laid_off) as total
FROM layoffs_staging2
WHERE YEAR(`date`) = 2021
GROUP BY company
ORDER BY 2 DESC;

-- Ranks the Top 5 companies with the most layoffs each year in order
WITH company_year_cte (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,  YEAR(`date`)
), 
company_year_rank AS
(
SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year_cte
WHERE years IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE ranking <= 5;