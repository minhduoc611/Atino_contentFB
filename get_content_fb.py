import requests
import json
from datetime import datetime, timedelta
import time

class FacebookToLarkBase:
    def __init__(self, fb_access_token, fb_page_id, lark_app_id, lark_app_secret):
        self.fb_access_token = fb_access_token
        self.fb_page_id = fb_page_id
        self.lark_app_id = lark_app_id
        self.lark_app_secret = lark_app_secret
        self.fb_base_url = "https://graph.facebook.com/v21.0"
        self.lark_base_url = "https://open.feishu.cn/open-apis"
        self.tenant_access_token = None
    
    def get_lark_tenant_access_token(self):
        url = f"{self.lark_base_url}/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.lark_app_id, "app_secret": self.lark_app_secret}
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if data.get("code") == 0:
                return data.get("tenant_access_token")
            else:
                print(f"Loi lay token: {data}")
                return None
        except Exception as e:
            print(f"Loi: {e}")
            return None
    
    def get_facebook_posts(self, since_date):
        url = f"{self.fb_base_url}/{self.fb_page_id}/posts"
        dt = datetime.strptime(since_date, "%Y-%m-%d")
        since_timestamp = int(dt.timestamp())
        
        params = {
            "access_token": self.fb_access_token,
            "fields": "id,created_time",
            "since": since_timestamp,
            "limit": 100
        }
        
        all_posts = []
        
        try:
            while True:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                posts = data.get("data", [])
                if not posts:
                    break
                
                all_posts.extend(posts)
                
                next_url = data.get("paging", {}).get("next")
                if not next_url:
                    break
                
                url = next_url
                params = {}
            
            return all_posts
        except Exception as e:
            print(f"Loi lay Facebook posts: {e}")
            return []
    
    def format_time_for_lark(self, iso_time):
        try:
            dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%S%z")
            return int(dt.timestamp() * 1000)
        except:
            return int(datetime.now().timestamp() * 1000)
    
    def extract_post_id(self, post_id_field):
        if not post_id_field:
            return None
        
        if isinstance(post_id_field, list):
            if len(post_id_field) > 0:
                if isinstance(post_id_field[0], dict):
                    return post_id_field[0].get("text", "")
                else:
                    return str(post_id_field[0])
        elif isinstance(post_id_field, dict):
            return post_id_field.get("text", "")
        else:
            return str(post_id_field)
        
        return None
    
    def get_existing_records(self, app_token, table_id):
        if not self.tenant_access_token:
            self.tenant_access_token = self.get_lark_tenant_access_token()
            if not self.tenant_access_token:
                return {}
        
        url = f"{self.lark_base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
        headers = {
            "Authorization": f"Bearer {self.tenant_access_token}",
            "Content-Type": "application/json"
        }
        
        existing_records = {}
        page_token = None
        
        try:
            while True:
                payload = {"page_size": 500}
                if page_token:
                    payload["page_token"] = page_token
                
                response = requests.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                
                if data.get("code") != 0:
                    print(f"Loi lay records: {data}")
                    break
                
                items = data.get("data", {}).get("items", [])
                
                for item in items:
                    post_id = self.extract_post_id(item.get("fields", {}).get("Post ID"))
                    record_id = item.get("record_id")
                    
                    if post_id and record_id:
                        existing_records[post_id] = record_id
                
                if not data.get("data", {}).get("has_more", False):
                    break
                
                page_token = data.get("data", {}).get("page_token")
                time.sleep(0.3)
            
            return existing_records
        except Exception as e:
            print(f"Loi lay existing records: {e}")
            return {}
    
    def create_records(self, app_token, table_id, posts):
        url = f"{self.lark_base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.tenant_access_token}",
            "Content-Type": "application/json"
        }
        
        success_count = 0
        batch_size = 100
        
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            
            records = []
            for post in batch:
                records.append({
                    "fields": {
                        "Post ID": post.get("id", ""),
                        "Thời gian đăng": self.format_time_for_lark(post.get("created_time", ""))
                    }
                })
            
            try:
                response = requests.post(url, headers=headers, json={"records": records})
                response.raise_for_status()
                data = response.json()
                
                if data.get("code") == 0:
                    success_count += len(records)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"Loi tao records: {e}")
        
        return success_count
    
    def update_records(self, app_token, table_id, updates):
        if not updates:
            return 0
        
        url = f"{self.lark_base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        headers = {
            "Authorization": f"Bearer {self.tenant_access_token}",
            "Content-Type": "application/json"
        }
        
        success_count = 0
        batch_size = 100
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            try:
                response = requests.post(url, headers=headers, json={"records": batch})
                response.raise_for_status()
                data = response.json()
                
                if data.get("code") == 0:
                    success_count += len(batch)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"Loi update records: {e}")
        
        return success_count
    
    def upsert_records(self, app_token, table_id, posts):
        if not self.tenant_access_token:
            self.tenant_access_token = self.get_lark_tenant_access_token()
            if not self.tenant_access_token:
                return (0, 0)
        
        existing_records = self.get_existing_records(app_token, table_id)
        
        posts_to_create = []
        posts_to_update = []
        
        for post in posts:
            post_id = post.get("id", "")
            if post_id in existing_records:
                posts_to_update.append({
                    "record_id": existing_records[post_id],
                    "fields": {
                        "Post ID": post_id,
                        "Thời gian đăng": self.format_time_for_lark(post.get("created_time", ""))
                    }
                })
            else:
                posts_to_create.append(post)
        
        created_count = 0
        updated_count = 0
        
        if posts_to_create:
            created_count = self.create_records(app_token, table_id, posts_to_create)
        
        if posts_to_update:
            updated_count = self.update_records(app_token, table_id, posts_to_update)
        
        return (created_count, updated_count)
    
    def sync(self, app_token, table_id, since_date=None, days=7):
        if since_date is None:
            since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        print(f"Bat dau dong bo tu ngay {since_date} ({days} ngay gan nhat)")
        
        posts = self.get_facebook_posts(since_date)
        
        if not posts:
            print("Khong co du lieu")
            return
        
        print(f"Da lay {len(posts)} bai dang")
        
        created, updated = self.upsert_records(app_token, table_id, posts)
        
        print(f"Ket qua: {created} tao moi, {updated} cap nhat")
        print(f"Tong: {created + updated}/{len(posts)} records")


def main():
    FB_ACCESS_TOKEN = "EAAQ29yjUqgoBPx9n3kL2PJZAzYq1tC7XoVL1ZCtVXF9LMJ4XVwaBD7rZCqjKAmVLY547rTjBzSfgSBr9j8ryZBMjFxHKd6xMm1gPzydWnb4bZAjHHACzaJmITfC78TRINHxCPMD6RLUdWdnY2trc5V7LiZCSen6GMJPMbjaFgYVJ3naA50NVZBlYSlK7cQ96g8T4DnYn4X8ljEARwKANWNZBTBrC"
    FB_PAGE_ID = "356056647882948"
    
    LARK_APP_ID = "cli_a8620f964a38d02f"
    LARK_APP_SECRET = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"
    LARK_APP_TOKEN = "CvCHbw40NaeRW8sZRF5lPM9Fgwg"
    LARK_TABLE_ID = "tblI6w1G8C3eDvqu"
    
    syncer = FacebookToLarkBase(
        fb_access_token=FB_ACCESS_TOKEN,
        fb_page_id=FB_PAGE_ID,
        lark_app_id=LARK_APP_ID,
        lark_app_secret=LARK_APP_SECRET
    )
    
    syncer.sync(
        app_token=LARK_APP_TOKEN,
        table_id=LARK_TABLE_ID,
        days=7
    )


if __name__ == "__main__":
    main()