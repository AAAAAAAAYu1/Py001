# coding=utf-8
import logging
import requests
import json
import time
import random
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = Path(__file__).parent


class GetBaiduMapInfo:
    def __init__(self):
        self.Ak = "" #填入AK       
        self.city = ""   #填入你想爬取的城市     
        self.keywords_list = ["美食", "餐厅", "饭店", "餐馆", "小吃", "火锅", "烧烤", "快餐", "中餐厅"]  # 多关键词
        self.url = "http://api.map.baidu.com/place/v2/search?query="
        self.set = "&page_size=20&scope=1&city_limit=false&region="  # scope=1扩大基础结果
        self.params = {'page_num': 0}
        self.items = []
        self.output_csv = BASE_DIR / '云浮市美食数据.csv'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }

    def get_data(self):
        """主抓取逻辑"""
        try:
            for keyword in self.keywords_list:
                logging.info(f"当前关键词: {keyword}")
                url = f"{self.url}{keyword}{self.set}{self.city}&output=json&ak={self.Ak}"
                if not self._process_keyword(url):
                    continue  # 单个关键词失败不影响整体
            self._save_data()
            return True
        except Exception as e:
            logging.error(f"抓取失败: {str(e)}")
            return False

    def _process_keyword(self, url):
        """处理单个关键词的抓取"""
        try:
            # 获取总页数（免费版最多20页）
            total_page = self._get_total_page(url)
            if total_page is None:
                return False

            # 分页抓取
            for page in range(total_page):
                self.params['page_num'] = page
                if not self._parse_page_data(url):
                    break  # 当前页解析失败跳过
                time.sleep(random.uniform(1.5, 3))
            return True
        except Exception as e:
            logging.error(f"关键词处理异常: {str(e)}")
            return False

    def _get_total_page(self, url):
        """获取总页数并处理API限制"""
        try:
            response = self._request_with_retry(url, {'page_num': 0})
            if not response:
                return None
            data = json.loads(response.text)
            if data.get('status') != 0:
                logging.error(f"API错误: {data.get('message')}")
                return None
            total = data.get('total', 0)
            total_page = min((total + 19) // 20, 20)  # 强制最多20页
            logging.info(f"总数据量: {total} 条, 抓取页数: {total_page} 页")
            return total_page
        except Exception as e:
            logging.error(f"获取总页数失败: {str(e)}")
            return None

    def _parse_page_data(self, url):
        """解析单页数据"""
        try:
            response = self._request_with_retry(url, self.params)
            if not response:
                return False
            data = json.loads(response.text)
            for item in data.get('results', []):
                record = (
                    item.get('name', ''),
                    item.get('province', ''),
                    item.get('city', ''),
                    item.get('area', ''),
                    item.get('address', '')
                )
                self.items.append(record)
                logging.debug(f"已抓取: {record[0]}")
            return True
        except Exception as e:
            logging.error(f"解析失败: {str(e)}")
            return False

    def _request_with_retry(self, url, params, max_retries=3):
        """带重试机制的请求"""
        for _ in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=self.headers, timeout=10)
                if response.status_code == 200:
                    return response
            except Exception as e:
                logging.warning(f"请求异常: {str(e)}")
            time.sleep(random.uniform(2, 5))
        logging.error(f"请求失败: {url}")
        return None

    def _save_data(self):
        """保存数据并去重"""
        try:
            df = pd.DataFrame(self.items, columns=['店名', '省份', '城市', '区域', '详细地址'])
            df.drop_duplicates(subset=['店名', '详细地址'], inplace=True)  # 联合去重
            df.to_csv(self.output_csv, index=False, encoding='utf-8-sig')
            logging.info(f"已保存{len(df)}条数据，文件路径: {self.output_csv}")
        except Exception as e:
            logging.error(f"保存失败: {str(e)}")


def main():
    crawler = GetBaiduMapInfo()
    if crawler.get_data():
        logging.info("数据抓取成功！")
    else:
        logging.error("数据抓取失败")


if __name__ == '__main__':
    main()
