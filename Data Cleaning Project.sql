-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


select*
from layoffs;
-- 1. Remove Duplicates
-- 2 Standerdize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any columns


-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

Create Table layoffs_staging
Like layoffs;



Insert layoffs_staging
select*
from layoffs;

-- 1. Remove Duplicates

select * 
from layoffs_staging;

-- one solution, Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column.


select *,
Row_number() Over(partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;


WITH Duplicate_cte As
(
select *,
Row_number() Over(partition by company, location,
 industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select*
from duplicate_cte
where row_num > 1;

select*
from layoffs_staging
where company = 'casper';

-- but it is better to delete (modify) in a new table.

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

select*
from layoffs_staging2;

Insert Into layoffs_staging2
select *,
Row_number() Over(
partition by company, location,
 industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

Delete
from layoffs_staging2
where row_num > 1;
select*
from layoffs_staging2
where row_num > 1;


-- 2.Standardizing data

Select Distinct(trim(company))
from layoffs_staging2;


Update layoffs_staging2
set company = trim(company);



select *
from layoffs_staging2;

select distinct industry
from layoffs_staging2;

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

select *
from layoffs_staging2
where industry like'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';


-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

select*
from layoffs_staging2
where country Like 'United States%';

Update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Let's also fix the date columns:

select`date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2
;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- now we can convert the data type properly

alter table layoffs_staging2
modify column `date` date;



-- Null Values & MIssing Values


select *
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase


Select *
from layoffs_staging2
where industry IS NULL
Or Industry  = '';

 -- it looks like airbnb is a travel, but this one just isn't populated.
 
select *
from layoffs_staging2
where company = 'Airbnb';

 -- write a query that if there is another row with the same company name, it will update it to the non-null industry values
 
Select t1.industry, t2.industry
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
where (t1.industry is  NULL or t1.industry = '')
and t2.industry is not null;
 
 -- we should set the blanks to nulls since those are typically easier to work with
 
Update layoffs_staging2
set industry = NULL
Where industry = '';

-- Populate the Industry

update layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is NULL
and t2.industry is not null;





select *
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use

delete
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;


alter table layoffs_staging2
drop column row_num;


select*
from layoffs_staging2;











