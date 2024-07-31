-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or Blank values
-- 4. Remove any unneccessary columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER 
	(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
	AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

select *
FROM layoffs_staging
WHERE company = 'Cazoo';


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
SELECT *
FROM layoffs_staging2
WHERE row_num > 2;

-- INSERTING A TABLE INTO ANOTHER

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER 
	(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
	AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
where row_num >1;

CREATE TABLE `layoffs_staging3` (
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

SELECT *
FROM layoffs_staging3
WHERE row_num > 1;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER 
	(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
	AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging3
WHERE row_num > 1;
SELECT *
FROM layoffs_staging3
WHERE row_num > 1;

-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging3
ORDER BY 1;

SELECT *
FROM layoffs_staging3
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging3
SET industry = 'Crypto'
Where industry LIKE 'Crypto%';

SELECT  DISTINCT country
FROM layoffs_staging3
ORDER BY 1;

UPDATE layoffs_staging3
SET country = 'United States'
Where country LIKE 'United States%';

-- CONVERTING DATE TEXT TO REAL DATE FOR DATA.
SELECT `date`
-- STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3
WHERE industry IS NULL OR industry = "";

SELECT *
FROM layoffs_staging3
WHERE company = 'Airbnb';

UPDATE layoffs_staging3
SET industry = null
WHERE industry = '';


SELECT *
FROM layoffs_staging3 L1
JOIN layoffs_staging3 L2
	ON L1.company = L2.company
WHERE L1.industry IS NULL AND L2.industry IS NOT NULL;

UPDATE layoffs_staging3 L1
JOIN layoffs_staging3 L2
	ON L1.company = L2.company
SET L1.industry = L2.industry
WHERE L1.industry IS NULL AND L2.industry IS NOT NULL;


DELETE
FROM layoffs_staging3
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num;

-- EXPLANATORY DAT
SELECT *
FROM layoffs_staging3;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging3;

SELECT *
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company
ORDER BY 2 DESC;

SELECT  MIN(`date`), MAX(`date`)
FROM layoffs_staging3;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging3
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH`;

WITH rolling_total AS (
	SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
	FROM layoffs_staging3
	WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
	GROUP BY `MONTH`
	ORDER BY `MONTH`
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) rolling_total
FROM rolling_total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company, YEAR(`date`)
ORDER BY company;

WITH company_cte (company, years, total_laid_off) AS(
	SELECT company, YEAR(`date`), SUM(total_laid_off)
	FROM layoffs_staging3
	GROUP BY company, YEAR(`date`)
),
Company_year_ranking AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY years Order by total_laid_off DESC) AS Y_TLO
FROM company_cte
WHERE years IS NOT NULL)
SELECT *
FROM Company_year_ranking
WHERE Y_TLO <= 5;