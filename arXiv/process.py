import os
import re
import time
import json
import logging
from datetime import datetime
from configparser import ConfigParser
from collections import defaultdict

import arxiv
from fake_useragent import UserAgent
import requests
from retrying import retry

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arxiv_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

cwd = os.path.abspath(os.path.dirname(__file__))
config_path = os.path.abspath(os.path.join(cwd, '..', 'config.ini'))

config = ConfigParser()
config.read(config_path)

sort_by_dict = {'relevance': arxiv.SortCriterion.Relevance,
                'lastUpdatedDate': arxiv.SortCriterion.LastUpdatedDate,
                'submittedDate': arxiv.SortCriterion.SubmittedDate}

sort_order_dict = {'descending': arxiv.SortOrder.Descending,
                    'ascending': arxiv.SortOrder.Ascending}


def load_set(subject):
    arxiv_db_path = os.path.abspath(os.path.join(cwd, '..', 'arXiv_db', subject))
    arxiv_db_set = os.path.join(arxiv_db_path, 'db.txt')
    if not os.path.exists(arxiv_db_path):
        # 第一次运行
        os.makedirs(arxiv_db_path)
        return set(), arxiv_db_path
    elif not os.path.exists(arxiv_db_set):
        return set(), arxiv_db_path
    else:
        # 读取已存在的
        with open(arxiv_db_set, "r") as f:
            tmp = json.loads(f.read())
        return set(tmp), arxiv_db_path


def load_markdown(markdown_fp):
    with open(markdown_fp, "r", encoding='utf-8') as f:
        raw_markdown = f.read()

    prog = re.compile('<summary>(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (.*)<\/summary>\n\n- \*(.+)\*\n\n- `(.+)`.* \[pdf\]\((.+)\)\n\n> (.+)\n\n<\/details>')
    matches = prog.findall(raw_markdown)

    results = []

    for result in matches:
        ori = {}
        ori['title'] = result[1]
        ori['authors'] = result[2].split(', ')
        ori['updated_sorted'] = time.strptime(result[0], '%Y-%m-%d %H:%M:%S')
        ori['updated'] = result[0]
        ori['summary'] = result[5]
        ori['pdf_url'] = result[4]
        ori['short_id'] = result[3]
        results.append(ori)
    return results


@retry(stop_max_attempt_number=3, wait_fixed=10000)
def create_arxiv_client(page_size, delay_seconds, num_retries):
    """创建ArXiv客户端，带重试机制"""
    try:
        # 创建带User-Agent的session
        session = requests.Session()
        ua = UserAgent()
        session.headers.update({
            'User-Agent': ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        client = arxiv.Client(
            page_size=int(page_size),
            delay_seconds=int(delay_seconds),
            num_retries=int(num_retries),
            http_client=session
        )
        
        return client
        
    except Exception as e:
        logger.error(f"创建ArXiv客户端失败: {e}")
        raise


def crawler(query,
            sort_by,
            sort_order,
            page_size,
            subjectcategory,
            max_results=float('inf')):
    """
    改进的爬虫函数，支持错误处理和重试机制
    """
    # 参数处理
    query = json.loads(query)
    subjectcategory = json.loads(subjectcategory)
    max_results = int(max_results) if isinstance(max_results, str) else max_results

    logger.info(f"开始爬取，查询: {query}, 最大结果数: {max_results}")

    try:
        # 创建客户端
        client = create_arxiv_client(page_size, 5, 5)
        logger.info("ArXiv客户端创建成功")
    except Exception as e:
        logger.error(f"无法创建ArXiv客户端: {e}")
        return

    for subject, key_words in query.items():
        logger.info(f"开始处理主题: {subject}")
        query_results = defaultdict(list)
        db_set, arxiv_db_path = load_set(subject)
        logger.info(f"已加载 {len(db_set)} 条历史记录")

        # 每个关键字一个查询请求
        for key_word in key_words:
            logger.info(f"搜索关键词: {key_word}")
            
            search = arxiv.Search(
                query=key_word,
                max_results=max_results,
                sort_by=sort_by_dict[sort_by],
                sort_order=sort_order_dict[sort_order]
            )

            try:
                paper_count = 0
                for result in client.get(search):
                    # 是否在指定的类别内
                    for cate in result.categories:
                        if cate in subjectcategory:
                            break
                    else:
                        continue

                    # 数据库中是否已存在
                    short_id = result.get_short_id()
                    if short_id in db_set:
                        continue
                    db_set.add(short_id)

                    year = result.updated.tm_year
                    ori = dict()
                    ori['title'] = result.title
                    ori['authors'] = [author.name for author in result.authors]
                    ori['updated_sorted'] = result.updated
                    ori['updated'] = time.strftime('%Y-%m-%d %H:%M:%S', result.updated)
                    ori['summary'] = result.summary.replace('\n', ' ')
                    ori['pdf_url'] = result.get_pdf_url()
                    ori['short_id'] = result.get_short_id()
                    query_results[year].append(ori)
                    paper_count += 1
                    
                    # 每处理100篇论文记录一次
                    if paper_count % 100 == 0:
                        logger.info(f"已处理 {paper_count} 篇论文")

                logger.info(f"关键词 '{key_word}' 找到 {paper_count} 篇新论文")
                
            except arxiv.UnexpectedEmptyPageError:
                logger.warning(f"{subject}--{key_word}: arxiv.UnexpectedEmptyPageError")
                time.sleep(10)  # 增加延迟重试
                
            except arxiv.HTTPError as e:
                logger.error(f"{subject}--{key_word}: arxiv.HTTPError - {e}")
                time.sleep(15)  # HTTP错误增加延迟
                
            except requests.exceptions.RequestException as e:
                logger.error(f"{subject}--{key_word}: 网络请求错误 - {e}")
                time.sleep(20)
                
            except Exception as error:
                logger.error(f"{subject}--{key_word}: 未知错误 - {error}")
                time.sleep(10)

        # 解析存储结果
        total_new_papers = sum(len(results) for results in query_results.values())
        logger.info(f"主题 '{subject}' 总共找到 {total_new_papers} 篇新论文")
        
        for year, results in query_results.items():
            markdown_fp = os.path.join(arxiv_db_path, f'{year}.md')
            if os.path.exists(markdown_fp):
                old_results = load_markdown(markdown_fp)
                query_set = set([item['short_id'] for item in old_results])
                for item in results:
                    if item['short_id'] not in query_set:
                        old_results.append(item)
                results = old_results
            results = sorted(results, key=lambda item: item['updated_sorted'])

            markdown = []
            markdown.append(f"# {year}\n")

            toc = []
            content = defaultdict(list)
            for result in results:
                ym = result['updated'].rsplit('-', 1)[0]
                if ym not in toc:
                    toc.append(ym)
                paper = f"<details>\n\n<summary>{result['updated']} - {result['title']}</summary>\n\n" \
                        f"- *{', '.join(result['authors'])}*\n\n" \
                        f"- `{result['short_id']}` - [abs](http://arxiv.org/abs/{result['short_id']}) - [pdf]({result['pdf_url']})\n\n" \
                        f"> {result['summary']}\n\n" \
                        f"</details>\n\n"
                content[ym].append(paper)

            markdown.append("## TOC\n")
            toc = sorted(toc)
            markdown.append("\n".join([f"- [{t}](#{t})" for t in toc])+'\n')

            for ym, papers in content.items():
                markdown.append(f"## {ym}\n")
                markdown.append("".join(papers))

            with open(markdown_fp, "w", encoding='utf-8') as f:
                f.write("\n".join(markdown))
                
            logger.info(f"已更新 {year} 年的数据，共 {len(results)} 篇论文")

        if len(query_results) > 0:
            with open(os.path.join(arxiv_db_path, 'db.txt'), "w") as f:
                db_str = json.dumps(list(db_set))
                f.write(db_str)
            logger.info(f"已更新 {subject} 主题的数据库记录")

    logger.info("爬取完成！")


if __name__ == '__main__':
    logger.info("=" * 50)
    logger.info(f"开始运行Paper_Crawler - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
    
    try:
        crawler(**dict(config.items("arXiv")))
        logger.info("=" * 50)
        logger.info("Paper_Crawler 运行完成！")
        logger.info("=" * 50)
    except Exception as e:
        logger.error(f"运行过程中出现错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
