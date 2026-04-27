import requests
import csv
import time

# List of Reddit URLs you want to scrape
# (Truncated here for brevity, add the rest of your links to this list)
urls = [
    "https://www.reddit.com/r/UIUC/comments/tnxolc/how_are_here_apartments/",
    "https://www.reddit.com/r/UIUC/comments/9mjhe2/how_are_here_apartments/",
    "https://www.reddit.com/r/UIUC/comments/mg7zed/opinions_on_here_apartments/",
    "https://www.reddit.com/r/UIUC/comments/1l0b45y/dean_vs_hub/",
    "https://www.reddit.com/r/UIUC/comments/spqi6b/how_is_the_hub_apartment/",
    "https://www.reddit.com/r/UIUC/comments/165zi45/any_thoughts_on_hub/",
    "https://www.reddit.com/r/UIUC/comments/xbz5b7/uiuc_housing_the_hub_vs_the_deango/",
    "https://www.reddit.com/r/UIUC/comments/1cxygbb/should_i_sign_the_deans_the_hub/",
    "https://www.reddit.com/r/UIUC/comments/l3ne0n/is_the_dean_actually_that_bad/",
    "https://www.reddit.com/r/UIUC/comments/xkbkf1/warning_do_not_sign_at_the_dean/",
    "https://www.reddit.com/r/UIUC/comments/1fvftwz/damn_dean_campustown_management_like_that/",
    "https://www.reddit.com/r/UIUC/comments/1nznznt/best_luxury_apartments_on_green_need_suggestions/",
    "https://www.reddit.com/r/UIUC/comments/jb9iln/the_dean_campustown_rant/",
    "https://www.reddit.com/r/UIUC/comments/s3fyv0/the_dean_apartments/",
    "https://www.reddit.com/r/UIUC/comments/m1zviu/icon_luxury_student_living_ready_august_2021/",
    "https://www.reddit.com/r/UIUC/comments/1n1gouf/bestworst_off_campus_apts/",
    "https://www.reddit.com/r/UIUC/comments/1g51nty/thoughts_on_yugo_urbana/",
    "https://www.reddit.com/r/UIUC/comments/13qs9em/yugo_apartments/",
    "https://www.reddit.com/r/UIUC/comments/13qm01i/thoughts_and_reviews_of_yugo_urbana/",
    "https://www.reddit.com/r/UIUC/comments/1ea4elr/any_recent_reviewsthoughts_on_living_on_yugo/",
    "https://www.reddit.com/r/UIUC/comments/1furmj0/honest_thoughts_on_yugo_in_champaign/",
    "https://www.reddit.com/r/UIUC/comments/1hjb3bj/living_at_latitude/",
    "https://www.reddit.com/r/UIUC/comments/15hp7ac/about_latitude_apartments/",
    "https://www.reddit.com/r/UIUC/comments/lx9oeq/latitude_apartments_post_march_2020_reviews/",
    "https://www.reddit.com/r/UIUC/comments/1qtn7xl/latitude_the_worse_appartment/",
    "https://www.reddit.com/r/UIUC/comments/gxf6cg/latitude_apartments_the_last_housing_you_should/",
    "https://www.reddit.com/r/UIUC/comments/16mb5lj/leasing_company_tier_list/",
    "https://www.reddit.com/r/UIUC/comments/hpll9j/tower_at_third_reviews/",
    "https://www.reddit.com/r/UIUC/comments/1cr3hx6/do_not_live_at_tower_at_third/",
    "https://www.reddit.com/r/UIUC/comments/y6pm5a/is_tower_at_3rd_worth_it_do_any_pros_outweigh_the/",
    "https://www.reddit.com/r/UIUC/comments/smflrj/707_is_the_best_apartment_complex_on_campus/",
    "https://www.reddit.com/r/UIUC/comments/sm59zv/fuck_707/",
    "https://www.reddit.com/r/UIUC/comments/smeir5/conflicted_feelings_on_707_situation/",
    "https://www.reddit.com/r/UIUC/comments/1o01vpr/thoughts_on_octave_apartments/",
    "https://www.reddit.com/r/UIUC/comments/1o58pw4/beware_octave_apartments_lease_and_switch/",
    "https://www.reddit.com/r/UIUC/comments/qbhahy/the_apartment_in_the_campus/",
    "https://www.reddit.com/r/UIUC/comments/u88rsn/octave_sucks_dont_waste_your_time_on_this/",
    "https://www.reddit.com/r/UIUC/comments/nh6msa/75_armory/",
    "https://www.reddit.com/r/UIUC/comments/1ddwnwp/armory_house_at_uiuc_is_there_a_good_social_scene/",
    "https://www.reddit.com/r/UIUC/comments/v63xcl/illini_manor_is_it_a_good_apartment/",
    "https://www.reddit.com/r/UIUC/comments/pbkthz/absolute_worst_places_to_stay_on_campus/",
    "https://www.reddit.com/r/UIUC/comments/1dalccg/illini_tower_in_depth_review_from_a_resident_and/",
    "https://www.reddit.com/r/UIUC/comments/1bzpwkf/is_illini_tower_a_fine_place_to_live_for_freshman/"
]

# CRITICAL: Reddit blocks default Python requests. You must spoof a standard web browser User-Agent.
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Prepare the data list with column headers
extracted_data = [["Subreddit", "Post Title", "Original URL", "Author", "Comment Text"]]

print("Starting scraper...")

for url in urls:
    # Format the URL to hit the JSON endpoint
    json_url = url.rstrip('/') + '.json'
    print(f"Scraping: {json_url}")
    
    try:
        # Fetch the data
        response = requests.get(json_url, headers=headers)
        
        # If we get rate limited (429), pause and retry
        if response.status_code == 429:
            print("Rate limited by Reddit. Sleeping for 5 seconds...")
            time.sleep(5)
            response = requests.get(json_url, headers=headers)
            
        response.raise_for_status()
        data = response.json()
        
        # Reddit JSON returns a list: [0] is the post info, [1] is the comments
        post_info = data[0]['data']['children'][0]['data']
        post_title = post_info.get('title', 'Unknown Title')
        subreddit = post_info.get('subreddit', 'Unknown Subreddit')
        
        # Extract top-level comments
        comments_list = data[1]['data']['children']
        
        for comment in comments_list:
            # 't1' indicates a standard comment object (ignores 'MoreComments' objects)
            if comment.get('kind') == 't1':
                comment_data = comment['data']
                author = comment_data.get('author', '[deleted]')
                body = comment_data.get('body', '').replace('\n', ' ').strip()
                
                # Append to our dataset if the comment isn't empty
                if body:
                    extracted_data.append([subreddit, post_title, url, author, body])
                    
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve {url}: {e}")
    except (IndexError, KeyError) as e:
        print(f"Data structure error on {url}: {e}")
        
    # Politeness delay to avoid IP bans
    time.sleep(2)

# Save the extracted data to a CSV file
output_filename = "reddit_housing_raw_data.csv"

with open(output_filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerows(extracted_data)

print(f"\nScraping complete! Data saved to {output_filename}")
