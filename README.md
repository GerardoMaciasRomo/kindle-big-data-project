# 📘 Big Data Project – Amazon Kindle Reviews Analysis

## 👤 Author
Gerardo Macías Romo  
Big Data – Intermediate Project  

---

# 📌 Project Overview

This project implements an end-to-end Big Data pipeline using **Apache Spark / PySpark** to analyze over **2.2 million Amazon Kindle book reviews**.

The goal is to extract insights from user reviews, understand rating behavior, and apply both **batch processing, streaming simulation, and machine learning techniques**.

---

# 🎯 Problem Statement

Digital reading platforms like Amazon Kindle contain millions of reviews, making it difficult to:

- Identify high-quality books
- Understand user satisfaction
- Extract meaningful insights from unstructured text data

This project addresses these challenges using a scalable Big Data pipeline.

---

# 🧱 Project Architecture

RAW DATA
↓
INGESTION LAYER (Spark Config + Validation)
↓
PROCESSING LAYER (Cleaning & Transformation)
↓
CURATED LAYER (Aggregations & Metrics)
↓
BATCH PROCESSING (Feature Engineering)
↓
STREAMING SIMULATION (Incremental processing)
↓
ANALYTICAL DATA MODEL (Database storage)
↓
VISUALIZATION LAYER (Insights & Charts)
↓
ML LAYER (Clustering Analysis)

# 📁 Project Structure

project/
│
├── data/
│ ├── raw/ ← Dataset downloaded here
│ ├── processed/
│ ├── curated/
│ ├── ai/
│ ├── logs/
│
│
├── spark_config.py
├── maintain_ingestion.py
├── data_processed.py
├── data_curated.py
├── batch_processing.py
├── pyspark_streaming.py
├── analytical_data_model.py
├── data_visualization.py
├── data_clustering.py
│
├── top_books.png
├── analytics.db
├── dashboard.png
│
└── README.md

---

# 📥 Dataset Download Instructions

The dataset is NOT included due to its large size (>2 million records).

## 🔗 Source:
Amazon Review Dataset  
https://nijianmo.github.io/amazon/index.html  

## 📦 Dataset Used:
Kindle Store 5-core dataset

## 📁 Installation Steps:

1. Download the dataset from the link above  
2. Extract it into the following folder:

data/raw/Kindle_Store_5/


3. Ensure JSON files are inside this directory

---

# ⚙️ Environment Setup

Install dependencies:

```bash
pip install pyspark pandas matplotlib seaborn scikit-learn

# ▶️ Execution Order (IMPORTANT)
Run scripts in the following order:

1.-spark_config.py
2.-maintain_ingestion.py
3.-data_processed.py
4.-data_curated.py
5.-processing.py
6.-pyspark_streaming.py
7.-analytical_data_model.py
8.-data_visualization.py
9.-data_clustering.py

# 🧹 Data Processing Pipeline

## ✔ Data Cleaning
- Removed null values
- Filtered invalid ratings (1–5 range)
- Ensured schema consistency across all records

## ✔ Transformations
- Text normalization (lowercasing and punctuation removal)
- Date conversion from Unix timestamp
- Feature extraction (review length, review date)

## ✔ Feature Engineering
- Sentiment classification based on rating:
  - Positive → 4–5 stars
  - Neutral → 3 stars
  - Negative → 1–2 stars
- Generated additional features:
  - review_length
  - review_date

---

# 📊 Analytics & Insights

## 📌 Key Insights
- Ratings distribution is highly skewed toward 5 stars
- Most users provide positive feedback
- Longer reviews are more common in neutral or negative ratings

## 📌 Explanation
- Amazon review bias: satisfied users are more likely to leave reviews
- Popular books dominate the dataset, increasing positive ratings
- Users with stronger opinions tend to write longer reviews

## 📌 Recommendations
- Improve recommendation systems using sentiment-aware filtering
- Identify low-rated books for quality improvement analysis
- Segment users based on behavior for targeted marketing strategies

---

# 🤖 Machine Learning (Clustering)

A K-Means clustering model was applied to segment books/users into behavioral groups based on:

- Average rating
- Review length
- Number of reviews

## Insight:
The data naturally separates into 3 clusters:
- High satisfaction group
- Neutral engagement group
- Low satisfaction / critical group

## Business Use:
- Personalized recommendations
- Customer segmentation
- Review quality analysis

---

# 🗄️ Analytical Data Model

A structured analytical model was created to support querying and reporting.

## Design:
- Fact table: reviews
- Dimensions:
  - books (asin)
  - users (reviewerID)
  - sentiment classification

## Stored Outputs:
- Aggregated book statistics
- Rating distributions
- Sentiment summaries

All structured data was stored in Parquet / SQLite for fast querying.

---

# 📈 Visualization

The following visualizations were generated:

- Distribution of ratings (1–5 stars)
- Top-rated books by average rating
- Sentiment distribution
- Review length distribution

All charts are saved in:

outputs/charts/


## Insights from Visuals:
- Strong bias toward 5-star ratings
- Clear separation in sentiment groups
- Most books have small variance in ratings

---

# ⚡ Streaming Simulation

A simulated streaming pipeline was implemented using micro-batches.

## Process:
- Data processed in small batches
- Incremental aggregation of ratings
- Real-time update of book statistics

## Output:
- Batch-level averages
- Incremental updates of book metrics
- Final aggregated view of dataset

---

# 🧠 Technologies Used

- Apache Spark / PySpark
- Python
- Pandas
- Matplotlib
- Scikit-learn
- SQLite (for analytical model)

---

# 🚀 Final Result

This project demonstrates a complete Big Data pipeline with:

✔ Scalable batch processing  
✔ Streaming simulation  
✔ Analytical data modeling  
✔ Machine learning clustering  
✔ Data visualization and insights generation  
✔ End-to-end reproducible architecture  

---

# 📌 Notes

- Dataset must be downloaded manually from the provided source
- All scripts must be executed in the correct order
- Ensure folder structure is maintained as described in the repository
