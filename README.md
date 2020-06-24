# ElectionCommittee-Processor

## Tracks and Analyzes Election Committee Contributions

## Problem Statement
Election Committees receive contributions all year round. There are four main sources of funding: Small Individual Contributors, Large Individual Contributors, Political Committees and Candidates own money. My project will help Campaign Marketing teams understand the distribution of these contributions with respect to location and time helping them launch more effecient fundraisers and marketing strategies.  

## Technologies Used
- AWS
  - EC2
  - S3 Bucket
  - Amazon RDS
  - Amazon QuickSight
- Python 3.6
  - Spyder / Jupyter Notebook
  - Pandas
  - Numpy
- MySQL
- Airflow
- Docker

## Data Description
Raw txt files are available on the Federal Elections Committee Website. These txt files are extracted and dumped into an s3 bucket from where they are further processed. These txt files are tab separated files where each row contains information about a contribtion made towards a committee.

## Pipeline
Below is my proposed pipeline
![pipeline](https://user-images.githubusercontent.com/48104421/85505590-a7af8b00-b5bc-11ea-9776-1ea19f9f3080.png)
## Slides
[link](https://docs.google.com/presentation/d/11tgObQu23-wopmqbK_SRQUFK9fxzm5Cb/edit#slide=id.p8)

