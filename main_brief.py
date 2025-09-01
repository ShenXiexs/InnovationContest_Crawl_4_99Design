import os
import pandas as pd
import time
import logging
from requests.exceptions import ProxyError, RequestException, ConnectionError, Timeout
from Crawl99designBrief import download_brief

# 设置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def download_with_retry(contest_url, contest_output_dir, csv_filename, max_retries=20, base_delay=1):
    """
    带重试机制的下载函数
    
    Args:
        contest_url: 比赛URL
        contest_output_dir: 输出目录
        csv_filename: CSV文件名
        max_retries: 最大重试次数，默认20次
        base_delay: 基础延迟时间（秒），默认1秒
    
    Returns:
        bool: 下载成功返回True，失败返回False
    """
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"尝试第 {attempt + 1} 次下载: {csv_filename}")
            download_brief(contest_url, contest_output_dir, csv_filename)
            logger.info(f"下载成功: {csv_filename}")
            return True
            
        except (ProxyError, ConnectionError, Timeout) as e:
            if attempt < max_retries:
                # 指数退避策略：每次重试延迟时间递增
                delay = base_delay * (2 ** attempt)
                logger.warning(f"下载失败 (第{attempt + 1}次尝试): {csv_filename}")
                logger.warning(f"错误信息: {str(e)}")
                logger.info(f"等待 {delay} 秒后重试...")
                time.sleep(delay)
            else:
                logger.error(f"下载彻底失败，已重试 {max_retries} 次: {csv_filename}")
                logger.error(f"最终错误信息: {str(e)}")
                return False
                
        except Exception as e:
            # 其他类型的异常，记录但不重试
            logger.error(f"未知错误，跳过重试: {csv_filename}")
            logger.error(f"错误信息: {str(e)}")
            return False
    
    return False

def main():
    # Set base paths
    output_dir_list = "/Users/samxie/Research/CrowdDeleRej/Data/ContestList/"  # Path for contest list CSV
    output_dir_Image = "/Users/samxie/Research/CrowdDeleRej/Data/ImageList/0828/"  # Path for images and summary CSV
    
    # Ensure output directories exist
    os.makedirs(output_dir_list, exist_ok=True)
    os.makedirs(output_dir_Image, exist_ok=True)
    
    # Path for aggregated CSV file
    all_contests_csv = os.path.join(output_dir_Image, "Contest_brief0828.csv")
    
    # 失败记录文件
    failed_contests_csv = os.path.join(output_dir_Image, "Failed_contests.csv")
    
    # Step 2: Load contest data from CSV
    contests_df_path = os.path.join(output_dir_list, "Contest_URL_All_0828.csv")
    contests_df = pd.read_csv(contests_df_path, dtype=str)
    
    # Load existing summary data if the file exists
    if os.path.exists(all_contests_csv):
        all_contests_df = pd.read_csv(all_contests_csv)
    else:
        all_contests_df = pd.DataFrame()
    
    # 加载或创建失败记录
    failed_contests = []
    if os.path.exists(failed_contests_csv):
        failed_df = pd.read_csv(failed_contests_csv)
        logger.info(f"加载了 {len(failed_df)} 条失败记录")
    else:
        failed_df = pd.DataFrame(columns=['ContestID', 'ContestName', 'ContestURL', 'FailureReason'])
    
    # 统计信息
    total_contests = len(contests_df)
    successful_downloads = 0
    failed_downloads = 0
    
    # Step 4: Iterate through each contest
    for index, row in contests_df.iterrows():
        contest_url = row['ContestURL']
        contest_id = row['ContestID']
        contest_name = row['ContestName']
        
        logger.info(f"处理进度: {index + 1}/{total_contests} - {contest_name} (ID: {contest_id})")
        
        # Create a directory for each contest
        contest_output_dir = os.path.join(output_dir_Image, contest_id)
        os.makedirs(contest_output_dir, exist_ok=True)
        
        # 检查是否已经存在成功下载的文件
        csv_filename = f"{contest_id}_contest"
        contest_csv_path = os.path.join(contest_output_dir, f"Submission_Contestant_{csv_filename}.csv")
        
        if os.path.exists(contest_csv_path):
            logger.info(f"文件已存在，跳过下载: {contest_name} (ID: {contest_id})")
            # 仍然需要添加到汇总数据中
            contest_df = pd.read_csv(contest_csv_path)
            all_contests_df = pd.concat([all_contests_df, contest_df], ignore_index=True)
            successful_downloads += 1
            continue
        
        # Step 5: Download images for the current contest with retry mechanism
        print(f"Downloading Brief for contest {contest_name} (ID: {contest_id})...")
        
        # 使用重试机制下载
        download_success = download_with_retry(
            contest_url, 
            contest_output_dir, 
            csv_filename,
            max_retries=20,  # 最多重试20次
            base_delay=2     # 基础延迟2秒
        )
        
        if download_success:
            successful_downloads += 1
            # Step 6: Append current contest's data to the aggregated DataFrame
            if os.path.exists(contest_csv_path):
                contest_df = pd.read_csv(contest_csv_path)
                all_contests_df = pd.concat([all_contests_df, contest_df], ignore_index=True)
                
                # Save interim progress to prevent data loss
                all_contests_df.to_csv(all_contests_csv, index=False)
                logger.info(f"已保存中间进度，累计处理 {successful_downloads} 个成功的比赛")
        else:
            failed_downloads += 1
            # 记录失败的比赛
            failed_contest_info = {
                'ContestID': contest_id,
                'ContestName': contest_name,
                'ContestURL': contest_url,
                'FailureReason': 'Max retries exceeded'
            }
            failed_contests.append(failed_contest_info)
            logger.error(f"记录失败比赛: {contest_name} (ID: {contest_id})")
        
        # 每处理10个比赛保存一次失败记录
        if len(failed_contests) > 0 and (index + 1) % 10 == 0:
            failed_df_temp = pd.DataFrame(failed_contests)
            failed_df = pd.concat([failed_df, failed_df_temp], ignore_index=True)
            failed_df.to_csv(failed_contests_csv, index=False)
            failed_contests = []  # 清空临时列表
            logger.info(f"已保存失败记录，当前失败数量: {failed_downloads}")
    
    # 保存最终的失败记录
    if failed_contests:
        failed_df_temp = pd.DataFrame(failed_contests)
        failed_df = pd.concat([failed_df, failed_df_temp], ignore_index=True)
        failed_df.to_csv(failed_contests_csv, index=False)
    
    # Step 7: Final save of the aggregated data and remove duplicates
    if not all_contests_df.empty:
        all_contests_df.drop_duplicates(subset='DesignID', keep='first', inplace=True)
        all_contests_df.to_csv(all_contests_csv, index=False)
        logger.info(f"聚合CSV文件已保存: {all_contests_csv}")
    
    # 输出最终统计信息
    logger.info("=" * 50)
    logger.info("爬取任务完成统计:")
    logger.info(f"总比赛数量: {total_contests}")
    logger.info(f"成功下载: {successful_downloads}")
    logger.info(f"失败下载: {failed_downloads}")
    logger.info(f"成功率: {successful_downloads/total_contests*100:.2f}%")
    
    if failed_downloads > 0:
        logger.info(f"失败记录已保存到: {failed_contests_csv}")
    
    print(f"Aggregated CSV file saved as {all_contests_csv}")

if __name__ == "__main__":
    main()