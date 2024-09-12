
![Logo](img/logo-white.png)


#  Fbref.com Football Data Scraper

This Python project aims to extract football data from the Fbref.com website, transform it, and store it in a PostgreSQL database for analysis purposes. The collected data can be used for advanced analysis, visualizations, or other football-related applications.
## Installation

Clone the project

```bash
  git clone https://github.com/Mouss1995/ballmetric.git
```

Go to the project directory 

```bash
  cd ballmetric
```

Create and activate virtual environment 

```bash
  python3 -m venv venv
  source venv/bin/activate 
```

Install dependencies

```bash
  pip install -r requirements.txt
```

## Setup a local PostgreSQL database
Set up a Postgresql server beforehand. The database connection information must be filled in when the script is launched, if this has not already been done.


## Run Locally
Run the main.py script

```bash
  ./main.py
```