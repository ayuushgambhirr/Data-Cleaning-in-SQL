select * 
from layoffs;

-- Remove duplicates
-- Standardize the Data
-- NUll values or Blank Values
-- Remove any Columns

Create Table layoffs_staging
like layoffs;
select * 
from layoffs_staging;

INSERT INTO layoffs_staging
Select *
from layoffs;

Select *
from layoffs_staging;

select *,
Row_Number() over(Partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

WITH duplicate_cte AS
(
select *,
Row_Number() over(Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
SELECT *
from duplicate_cte
where row_num > 1;

SELECT *
from layoffs_staging
where company = 'Casper';

DELETE
from duplicate_cte
where row_num > 1;

WITH duplicate_cte AS
(
select *,
Row_Number() over(Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
DELETE
from duplicate_cte
where row_num > 1;

DROP table IF exists layoffs_staging2;
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
from layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
select *,
Row_Number() over(Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;


DELETE
FROM layoffs_staging2
where row_num > 1;

SELECT * 
from layoffs_staging2;	

-- Standardizing Data

select company , Trim(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = Trim(company);

select Distinct industry
from layoffs_staging2
order by 1;

Update layoffs_staging2
Set industry = 'Crypto'
where industry LIKE 'Crypto%';

SELECT *
from layoffs_staging2
where country LIKE 'United States%';

Select Distinct country, TRIM(TRAILING '.' FRom country)
from layoffs_staging2
order by 1;

Update layoffs_staging2
Set country = TRIM(TRAILING '.' FRom country)
where country Like 'United States%';

Select `date`
from layoffs_staging2
order by 1;

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

Update layoffs_staging2
Set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

select * 
from layoffs_staging2;

Select `date`
from layoffs_staging2
where `date` is null;


-- Remove Null and Blank Values

SELECT *
from layoffs_staging2
where industry IS null or industry = '';

SELECT *
from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = null
where industry = '';

Select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
Set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

-- Exploratory Data Analysis

Select *
from layoffs_staging2;

Select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc ;

Select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by sum(total_laid_off) desc;

select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry;

Select *
from layoffs_staging2;

Select year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by year(`date`)
order by 1 desc;

Select year(`date`), country, sum(total_laid_off) 
from layoffs_staging2
group by year(`date`), country
having country = 'India'
order by 1 desc;

Select stage, sum(total_laid_off) 
from layoffs_staging2
group by stage
order by 2 desc;

select substring(`date`,1,7) as `MONTH`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,6,2) is not null
group by `MONTH`
order by 1 asc;

WITH rolling_total as
(
select substring(`date`,1,7) as `MONTH`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,6,2) is not null
group by `MONTH`
order by 1 asc
)
select `MONTH`, total_off, SUM(total_off) over(order by `MONTH`) as rolling_total
from rolling_total;

select company, year(`date`) as years, sum(total_laid_off) as total_offs
from layoffs_staging2
group by company, years
order by 3 desc;

With company_year as
(
select company, year(`date`) as years, sum(total_laid_off) as total_offs
from layoffs_staging2
group by company, years
order by 3 desc
), company_year_rank as
(select *, dense_rank() over(partition by years order by total_offs desc) as ranking 
from company_year
where years is not null
)
select*
from company_year_rank
where ranking<=5;

