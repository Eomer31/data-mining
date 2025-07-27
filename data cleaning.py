import pandas as pd
import numpy as np

# --- File 1: reddit_multi_source_posts_20250621_081206.csv ---

# Load the CSV file
df_multi_source = pd.read_csv('reddit_multi_source_posts_20250621_081206.csv')

# Handle missing values in 'gender' and 'age'
df_multi_source['gender'].fillna('Unknown', inplace=True)
df_multi_source['age'].fillna(-1, inplace=True) # Using -1 to denote missing age

# Fill missing values in 'text' and 'flair'
df_multi_source['text'].fillna('No Text', inplace=True)
df_multi_source['flair'].fillna('No Flair', inplace=True)

# Convert 'created_date' to datetime
df_multi_source['created_date'] = pd.to_datetime(df_multi_source['created_date'])

# Remove duplicate posts
df_multi_source.drop_duplicates(subset=['post_id'], inplace=True)

# Save the updated cleaned data
df_multi_source.to_csv('cleaned_with_age_gender_reddit_multi_source_posts.csv', index=False)


# --- File 2: reddit_data_comments_20250621_004521.csv ---

# Load the CSV file
df_comments = pd.read_csv('reddit_data_comments_20250621_004521.csv')

# Handle missing values in 'comment_gender' and 'comment_age'
df_comments['comment_gender'].fillna('Unknown', inplace=True)
df_comments['comment_age'].fillna(-1, inplace=True) # Using -1 to denote missing age

# Convert 'comment_created_date' to datetime
df_comments['comment_created_date'] = pd.to_datetime(df_comments['comment_created_date'])

# Clean 'post_id' and 'comment_parent_id' by removing prefixes
df_comments['post_id'] = df_comments['post_id'].str.replace('t3_', '')
df_comments['comment_parent_id'] = df_comments['comment_parent_id'].str.replace('t[13]_', '', regex=True)

# Remove duplicate comments
df_comments.drop_duplicates(subset=['comment_id'], inplace=True)

# Save the updated cleaned data
df_comments.to_csv('cleaned_with_age_gender_reddit_data_comments.csv', index=False)


# --- File 3: reddit_data_posts_20250621_004521.csv ---

# Load the CSV file
df_posts = pd.read_csv('reddit_data_posts_20250621_004521.csv')

# Handle missing values in 'gender' and 'age'
df_posts['gender'].fillna('Unknown', inplace=True)
df_posts['age'].fillna(-1, inplace=True) # Using -1 to denote missing age

# Fill missing values in 'flair'
df_posts['flair'].fillna('No Flair', inplace=True)

# Convert 'created_date' to datetime
df_posts['created_date'] = pd.to_datetime(df_posts['created_date'])

# Remove duplicate posts
df_posts.drop_duplicates(subset=['post_id'], inplace=True)

# Save the updated cleaned data
df_posts.to_csv('cleaned_with_age_gender_reddit_data_posts.csv', index=False)

print("All three files have been re-processed to include 'age' and 'gender' columns.")