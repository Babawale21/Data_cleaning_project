# Data_cleaning_project
SQL Data Cleaning and Analysis Project
This project involves data cleaning and analysing a dataset containing information on layoffs. The objective is to clean the data, remove duplicates, standardize values, and perform various analyses to derive insights. The SQL script provided demonstrates these steps using a series of SQL queries.

**Project Structure**
i)  Data Cleaning: Initial data preparation steps including removing duplicates and handling null or blank values.
ii) Data Standardization: Standardizing data values and converting date formats.
iii) Data Analysis: Performing various analyses to gain insights into layoffs, including aggregations and time-based analyses.

**SQL Scripts**
**Data Cleaning**
A) Remove Duplicates and Create Staging Tables
    (-- Create a staging table to hold the cleaned data
    CREATE TABLE layoffs_staging LIKE layoffs;
    
    -- Insert data into the staging table
    INSERT INTO layoffs_staging
    SELECT * FROM layoffs;
    
    -- Identify duplicates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging;)
    
B) Further Data Cleaning and Deduplication
    (CREATE TABLE layoffs_staging2;

    INSERT INTO layoffs_staging2
    SELECT *, ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging;

    -- Remove duplicates
    DELETE FROM layoffs_staging3 WHERE row_num > 1;)

**Data Standardization
Standardize Company Names and Industries**
    (-- Standardize company names
    UPDATE layoffs_staging3 SET company = TRIM(company);
    
    -- Standardize industry values
    UPDATE layoffs_staging3 SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';
    
    -- Standardize country values
    UPDATE layoffs_staging3 SET country = 'United States' WHERE country LIKE 'United States%';
    
    -- Convert date text to actual date format
    UPDATE layoffs_staging3 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
    ALTER TABLE layoffs_staging3 MODIFY COLUMN `date` DATE;)

**Data Analysis
Aggregate and Analyze Layoff Data**
    (-- Maximum and minimum values
    SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging3;
    
    -- Total layoffs by company
    SELECT company, SUM(total_laid_off) FROM layoffs_staging3 GROUP BY company ORDER BY 2 DESC;
    
    -- Total layoffs by industry and country
    SELECT industry, SUM(total_laid_off) FROM layoffs_staging3 GROUP BY industry ORDER BY 2 DESC;
    SELECT country, SUM(total_laid_off) FROM layoffs_staging3 GROUP BY country ORDER BY 2 DESC;
    
    -- Time-based analysis
    SELECT YEAR(`date`), SUM(total_laid_off) FROM layoffs_staging3 GROUP BY YEAR(`date`) ORDER BY 1 DESC;)

**Company Ranking by Layoffs**
      WITH company_cte AS (
          SELECT company, YEAR(`date`), SUM(total_laid_off)
          FROM layoffs_staging3
          GROUP BY company, YEAR(`date`)
      ),
      Company_year_ranking AS (
          SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Y_TLO
          FROM company_cte
          WHERE years IS NOT NULL
      )
      SELECT * FROM Company_year_ranking WHERE Y_TLO <= 5;


**How to Run the Script
Set Up Your Database**

    Ensure you have a MySQL database set up and accessible.
    Import SQL Script
    
    Copy and paste the SQL script into your MySQL command-line interface or a MySQL client like MySQL Workbench.
    Execute Queries
    
    Run each script section to clean, standardize, and analyze the data.

**Additional Information**
    Database Schema: Ensure the initial layoffs table exists in your database before running the script.
    Dependencies: This script assumes the use of MySQL and its functions for handling data types and date conversions.


    


