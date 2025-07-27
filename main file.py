import praw
import pandas as pd
import time
import re
import os
from datetime import datetime, timezone
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_age_gender(text):
    """Extract gender and age from post text"""
    if not text:
        return None, None
        
    text_lower = text.lower()
    
    # Simple patterns that are most likely to work
    patterns = [
        r'\b([mf])[,\s]*(\d{2})\b',  # M 25, F,30
        r'\b(\d{2})[,\s]*([mf])\b',  # 25 M, 30,F
        r'\bi.?m\s+(\d{2})\s+(male|female)\b',  # I'm 25 male
        r'\bi.?m\s+a\s+(\d{2})\s+year\s+old\s+(male|female)\b'  # I'm a 25 year old male
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            groups = match.groups()
            
            # Handle different pattern formats
            if len(groups) == 2:
                if groups[0] in ['m', 'f']:
                    gender = 'Male' if groups[0] == 'm' else 'Female'
                    try:
                        age = int(groups[1])
                    except:
                        continue
                elif groups[1] in ['m', 'f']:
                    gender = 'Male' if groups[1] == 'm' else 'Female'
                    try:
                        age = int(groups[0])
                    except:
                        continue
                elif groups[1] in ['male', 'female']:
                    gender = 'Male' if groups[1] == 'male' else 'Female'
                    try:
                        age = int(groups[0])
                    except:
                        continue
                else:
                    continue
                    
                if 16 <= age <= 80:
                    return gender, age
                    
    return None, None

class ImprovedRedditScraper:
    def __init__(self, client_id=None, client_secret=None, reddit_username=None):
        # Use provided credentials or defaults
        self.client_id = client_id or "St5Ln2XKuKmwmOKOmUZCmQ"
        self.client_secret = client_secret or "YtZw89rjpfHUpHWb_ahgBef241phsw"
        self.reddit_username = reddit_username or "plsgivemebloodvials"

        # Initialize counters
        self.total_posts_scraped = 0
        self.total_comments_scraped = 0
        self.posts_with_age_gender = 0
        self.comments_with_age_gender = 0
        self.api_calls_made = 0
        self.start_time = None

        user_agent = f"python:personalfinance_scraper:v1.0 (by /u/{self.reddit_username})"

        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=user_agent
            )
            logger.info("Reddit API initialized successfully!")
        except Exception as e:
            logger.error(f"Failed to initialize Reddit API: {e}")
            raise

    def test_connection(self):
        """Test Reddit API connection"""
        try:
            subreddit = self.reddit.subreddit('personalfinance')
            subscribers = subreddit.subscribers
            logger.info(f"Connection test successful. r/personalfinance has {subscribers:,} subscribers")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def print_stats(self):
        """Print comprehensive scraping statistics"""
        if self.start_time:
            elapsed_time = time.time() - self.start_time
            elapsed_minutes = elapsed_time / 60
        else:
            elapsed_time = 0
            elapsed_minutes = 0
            
        print("\n" + "="*60)
        print("SCRAPING STATISTICS")
        print("="*60)
        print(f"Total Runtime: {elapsed_minutes:.1f} minutes ({elapsed_time:.1f} seconds)")
        print(f"Total API Calls Made: {self.api_calls_made:,}")
        print(f"Average API Calls/Minute: {(self.api_calls_made / elapsed_minutes) if elapsed_minutes > 0 else 0:.1f}")
        print()
        print("POSTS:")
        print(f"  Total Posts Scraped: {self.total_posts_scraped:,}")
        print(f"  Posts with Age/Gender: {self.posts_with_age_gender:,} ({(self.posts_with_age_gender/self.total_posts_scraped*100) if self.total_posts_scraped > 0 else 0:.1f}%)")
        print()
        print("COMMENTS:")
        print(f"  Total Comments Scraped: {self.total_comments_scraped:,}")
        print(f"  Comments with Age/Gender: {self.comments_with_age_gender:,} ({(self.comments_with_age_gender/self.total_comments_scraped*100) if self.total_comments_scraped > 0 else 0:.1f}%)")
        print()
        print("EFFICIENCY:")
        if elapsed_time > 0:
            print(f"  Posts per Minute: {(self.total_posts_scraped / elapsed_minutes):.1f}")
            print(f"  Comments per Minute: {(self.total_comments_scraped / elapsed_minutes):.1f}")
            print(f"  Total Data Points per Minute: {((self.total_posts_scraped + self.total_comments_scraped) / elapsed_minutes):.1f}")
        print("="*60)

    def scrape_multiple_sources(self, target_posts=5000, batch_size=100):
        """
        Scrape from multiple sources to get more posts:
        - Hot posts
        - New posts  
        - Top posts from different time periods
        """
        self.start_time = time.time()
        
        if not self.test_connection():
            return []

        subreddit = self.reddit.subreddit('personalfinance')
        all_posts = []
        seen_ids = set()
        
        # Define different sources with higher limits for 5000 posts
        sources = [
            ('hot', lambda: subreddit.hot(limit=1500)),
            ('new', lambda: subreddit.new(limit=1500)), 
            ('top_week', lambda: subreddit.top(time_filter='week', limit=800)),
            ('top_month', lambda: subreddit.top(time_filter='month', limit=800)),
            ('top_year', lambda: subreddit.top(time_filter='year', limit=800)),
            ('top_all', lambda: subreddit.top(time_filter='all', limit=800)),
            ('rising', lambda: subreddit.rising(limit=500)),
            ('controversial_month', lambda: subreddit.controversial(time_filter='month', limit=300)),
        ]
        
        for source_name, source_func in sources:
            if len(all_posts) >= target_posts:
                break
                
            logger.info(f"Scraping from {source_name}...")
            
            try:
                posts = source_func()
                self.api_calls_made += 1
                batch_count = 0
                
                for post in posts:
                    if len(all_posts) >= target_posts:
                        break
                        
                    # Skip duplicates
                    if post.id in seen_ids:
                        continue
                    seen_ids.add(post.id)
                    
                    try:
                        post_data = self.process_post(post)
                        if post_data:  # Only add if we got valid data
                            all_posts.append(post_data)
                            batch_count += 1
                            self.total_posts_scraped += 1
                            
                            # Count posts with age/gender info
                            if post_data.get('age') is not None:
                                self.posts_with_age_gender += 1
                            
                            # Progress update
                            if batch_count % batch_size == 0:
                                logger.info(f"{source_name}: {batch_count} posts processed, {len(all_posts)} total")
                                logger.info(f"Running totals: {self.total_posts_scraped} posts, {self.posts_with_age_gender} with age/gender")
                                time.sleep(2)  # Respect rate limits
                                
                    except Exception as e:
                        logger.warning(f"Error processing post {post.id}: {e}")
                        continue
                        
                logger.info(f"Completed {source_name}: {batch_count} new posts")
                time.sleep(3)  # Longer delay between sources
                
            except Exception as e:
                logger.error(f"Error scraping {source_name}: {e}")
                continue
                
        return all_posts

    def process_post(self, post):
        """Process a single post and return data"""
        try:
            post_date = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
            
            # Get all text content
            title_text = post.title or ""
            body_text = post.selftext or ""
            combined_text = f"{title_text} {body_text}"
            
            # Extract age/gender
            gender, age = extract_age_gender(combined_text)
            
            post_data = {
                'post_id': post.id,
                'title': title_text[:500],  # Limit title length
                'text': body_text[:1000],  # Limit text length
                'author': str(post.author) if post.author else '[deleted]',
                'score': post.score,
                'upvote_ratio': post.upvote_ratio,
                'num_comments': post.num_comments,
                'created_date': post_date.strftime('%Y-%m-%d %H:%M:%S'),
                'url': post.url,
                'permalink': f"https://reddit.com{post.permalink}",
                'flair': post.link_flair_text,
                'gender': gender,
                'age': age,
                'has_selftext': bool(body_text),
                'text_length': len(combined_text)
            }
            
            return post_data
            
        except Exception as e:
            logger.error(f"Error processing post: {e}")
            return None
    
    def scrape_with_pagination(self, target_posts=5000):
        """
        Alternative approach using pagination to get more posts
        """
        if not self.start_time:
            self.start_time = time.time()
            
        if not self.test_connection():
            return []
            
        subreddit = self.reddit.subreddit('personalfinance')
        all_posts = []
        
        # Try different sorting methods
        sort_methods = ['hot', 'new', 'top']
        
        for sort_method in sort_methods:
            if len(all_posts) >= target_posts:
                break
                
            logger.info(f"Scraping using {sort_method} sorting...")
            
            try:
                if sort_method == 'hot':
                    posts = subreddit.hot(limit=None)  # None means get as many as possible
                elif sort_method == 'new':
                    posts = subreddit.new(limit=None)
                else:  # top
                    posts = subreddit.top(time_filter='all', limit=None)
                
                self.api_calls_made += 1
                batch_count = 0
                for post in posts:
                    if len(all_posts) >= target_posts:
                        break
                        
                    try:
                        post_data = self.process_post(post)
                        if post_data:
                            all_posts.append(post_data)
                            batch_count += 1
                            self.total_posts_scraped += 1
                            
                            # Count posts with age/gender info
                            if post_data.get('age') is not None:
                                self.posts_with_age_gender += 1
                            
                            if batch_count % 50 == 0:
                                logger.info(f"{sort_method}: {batch_count} posts, {len(all_posts)} total")
                                logger.info(f"Running totals: {self.total_posts_scraped} posts, {self.posts_with_age_gender} with age/gender")
                                time.sleep(1)  # Rate limiting
                                
                    except Exception as e:
                        logger.warning(f"Error with post: {e}")
                        continue
                        
                logger.info(f"Completed {sort_method}: {batch_count} posts")
                
            except Exception as e:
                logger.error(f"Error with {sort_method} sorting: {e}")
                continue
                
        return all_posts

    def get_limited_comments(self, post_id, max_comments=5):
        """Get a limited number of comments for a post"""
        try:
            post = self.reddit.submission(id=post_id)
            self.api_calls_made += 1
            post.comments.replace_more(limit=0)  # Don't expand MoreComments
            
            comments_data = []
            for i, comment in enumerate(post.comments[:max_comments]):
                if hasattr(comment, 'body') and comment.body not in ['[deleted]', '[removed]']:
                    comment_date = datetime.fromtimestamp(comment.created_utc, tz=timezone.utc)
                    gender, age = extract_age_gender(comment.body)
                    
                    comment_data = {
                        'comment_id': comment.id,
                        'post_id': post_id,
                        'comment_body': comment.body[:500],
                        'comment_author': str(comment.author) if comment.author else '[deleted]',
                        'comment_score': comment.score,
                        'comment_created_date': comment_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'comment_gender': gender,
                        'comment_age': age
                    }
                    comments_data.append(comment_data)
                    self.total_comments_scraped += 1
                    
                    # Count comments with age/gender info
                    if age is not None:
                        self.comments_with_age_gender += 1
                    
            return comments_data
            
        except Exception as e:
            logger.error(f"Error getting comments for post {post_id}: {e}")
            return []

    def save_data(self, posts_data, comments_data=None, filename_prefix='reddit_data'):
        """Save data to CSV files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        posts_df = None
        comments_df = None
        
        if posts_data:
            posts_df = pd.DataFrame(posts_data)
            posts_filename = f"{filename_prefix}_posts_{timestamp}.csv"
            posts_df.to_csv(posts_filename, index=False)
            logger.info(f"Posts saved to {posts_filename}")
            
        if comments_data:
            comments_df = pd.DataFrame(comments_data)
            comments_filename = f"{filename_prefix}_comments_{timestamp}.csv"
            comments_df.to_csv(comments_filename, index=False)
            logger.info(f"Comments saved to {comments_filename}")
            
        # Print final statistics
        self.print_stats()
        
        return posts_df, comments_df

    def scrape_posts_and_comments(self, target_posts=5000, comments_per_post=5):
        """
        Combined method to scrape both posts and comments with detailed counting
        """
        logger.info(f"Starting combined scraping: {target_posts} posts, {comments_per_post} comments per post")
        
        # First get posts
        posts = self.scrape_multiple_sources(target_posts=target_posts)
        
        if not posts:
            logger.error("No posts collected, skipping comment collection")
            return posts, []
            
        # Then get comments for posts with age/gender info (more valuable)
        comments = []
        posts_for_comments = [p for p in posts if p.get('age') is not None]
        
        if not posts_for_comments:
            # If no posts have age/gender, get comments from top-scoring posts
            posts_for_comments = sorted(posts, key=lambda x: x.get('score', 0), reverse=True)[:min(100, len(posts))]
        
        logger.info(f"Getting comments from {len(posts_for_comments)} selected posts...")
        
        for i, post in enumerate(posts_for_comments):
            try:
                post_comments = self.get_limited_comments(post['post_id'], comments_per_post)
                comments.extend(post_comments)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(posts_for_comments)} posts for comments")
                    logger.info(f"Comments collected so far: {self.total_comments_scraped}")
                    time.sleep(1)  # Rate limiting
                    
            except Exception as e:
                logger.warning(f"Error getting comments for post {post['post_id']}: {e}")
                continue
                
        return posts, comments

if __name__ == "__main__":
    try:
        scraper = ImprovedRedditScraper()
        
        # Method 1: Multiple sources
        logger.info("Starting multi-source scraping...")
        posts = scraper.scrape_multiple_sources(target_posts=5000)
        
        if posts:
            logger.info(f"Collected {len(posts)} posts from multiple sources")
            posts_df, _ = scraper.save_data(posts, filename_prefix='reddit_multi_source')
            
            # Show sample of results
            if posts_df is not None:
                print("\nSample of collected posts:")
                print(posts_df[['title', 'score', 'num_comments', 'age', 'gender', 'has_selftext']].head(10))
                
                # Show age/gender distribution
                age_gender_posts = posts_df[posts_df['age'].notna()]
                if len(age_gender_posts) > 0:
                    print(f"\nAge/Gender Distribution ({len(age_gender_posts)} posts):")
                    print(age_gender_posts['gender'].value_counts())
                    print(f"Age range: {age_gender_posts['age'].min()} - {age_gender_posts['age'].max()}")
        else:
            logger.error("No posts collected")
            
        # Optional: Try pagination method if multi-source didn't get enough
        if len(posts) < 4000:
            logger.info("Trying pagination method for more posts...")
            more_posts = scraper.scrape_with_pagination(target_posts=2000)
            if more_posts:
                logger.info(f"Got {len(more_posts)} additional posts")
                
    except Exception as e:
        logger.error(f"Script failed: {e}")
        import traceback
        traceback.print_exc()