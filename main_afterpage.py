import os
import pandas as pd
import time
import logging
import random
from requests.exceptions import RequestException, ProxyError, ConnectionError, Timeout, HTTPError, SSLError
from Crawl99designEntry import download_images

# 设置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('contest_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def download_contest_with_retry(contest_url, contest_output_dir, csv_filename, contest_id, contest_name, max_retries=20, base_delay=3):
    """
    带重试机制的比赛下载函数，专门处理网络连接问题
    
    Args:
        contest_url: 比赛URL
        contest_output_dir: 输出目录
        csv_filename: CSV文件名
        contest_id: 比赛ID
        contest_name: 比赛名称
        max_retries: 最大重试次数，默认20次
        base_delay: 基础延迟时间（秒），默认3秒
    
    Returns:
        tuple: (success: bool, error_message: str)
    """
    
    # 定义需要重试的异常类型
    retryable_exceptions = (
        ProxyError, 
        ConnectionError, 
        Timeout, 
        HTTPError,
        SSLError,
        RequestException
    )
    
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"尝试第 {attempt + 1}/{max_retries + 1} 次下载比赛: {contest_name} (ID: {contest_id})")
            
            # 在重试前添加随机延迟，避免请求过于密集
            if attempt > 0:
                # 指数退避 + 随机抖动
                base_wait = min(base_delay * (2 ** (attempt - 1)), 300)  # 最大延迟5分钟
                jitter = random.uniform(0.5, 1.5)  # 添加随机抖动
                wait_time = base_wait * jitter
                
                logger.info(f"等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)
            
            # 调用下载函数
            download_images(contest_url, contest_output_dir, csv_filename, nonactive=True)
            
            # 检查是否成功生成CSV文件
            contest_csv_path = os.path.join(contest_output_dir, f"Submission_Contestant_{csv_filename}.csv")
            
            if os.path.exists(contest_csv_path):
                # 检查CSV文件是否有有效内容
                try:
                    df = pd.read_csv(contest_csv_path)
                    if len(df) > 0:
                        # 进一步检查数据质量
                        valid_entries = df[df['DesignID'].notna() & (df['DesignID'] != 'N/A')]
                        if len(valid_entries) > 0:
                            logger.info(f" 下载成功: {contest_name} (ID: {contest_id}), 获得 {len(df)} 条记录 ({len(valid_entries)} 有效)")
                            return True, None
                        else:
                            logger.warning(f"CSV文件无有效数据: {contest_name} (ID: {contest_id})")
                    else:
                        logger.warning(f"CSV文件为空: {contest_name} (ID: {contest_id})")
                        
                except Exception as csv_error:
                    logger.warning(f"CSV文件读取失败: {contest_name} (ID: {contest_id}), 错误: {csv_error}")
                
                # 如果CSV存在但无效，删除它以便重试
                if attempt < max_retries:
                    try:
                        os.remove(contest_csv_path)
                        logger.info(f"删除无效CSV文件，准备重试")
                    except:
                        pass
            else:
                logger.warning(f"未生成CSV文件: {contest_name} (ID: {contest_id})")
            
            # 如果到这里说明本次尝试失败，记录错误信息
            last_error = "下载未完成或数据无效"
            
        except retryable_exceptions as e:
            last_error = str(e)
            error_type = type(e).__name__
            
            logger.warning(f" {error_type} (第{attempt + 1}次尝试): {contest_name} (ID: {contest_id})")
            logger.warning(f"错误详情: {last_error}")
            
            # 对于特定的错误类型，给出更详细的日志
            if isinstance(e, ProxyError):
                logger.warning("代理连接错误 - 可能是网络不稳定或代理服务器问题")
            elif isinstance(e, ConnectionError):
                logger.warning("连接错误 - 可能是网络中断或服务器无响应")
            elif isinstance(e, Timeout):
                logger.warning("超时错误 - 网络延迟过高")
            elif isinstance(e, SSLError):
                logger.warning("SSL错误 - 可能是证书问题")
            
            # 特殊处理：如果是连接重置错误，增加额外延迟
            if "Connection reset by peer" in str(e) or "ConnectionResetError" in str(e):
                if attempt < max_retries:
                    extra_delay = random.uniform(5, 10)
                    logger.info(f"检测到连接重置，额外等待 {extra_delay:.1f} 秒...")
                    time.sleep(extra_delay)
            
        except Exception as e:
            # 对于不可重试的异常，直接返回失败
            last_error = str(e)
            error_type = type(e).__name__
            logger.error(f" 不可重试的{error_type}错误: {contest_name} (ID: {contest_id})")
            logger.error(f"错误详情: {last_error}")
            return False, f"不可重试错误 ({error_type}): {last_error}"
    
    # 如果所有重试都失败了
    logger.error(f" 重试次数已耗尽: {contest_name} (ID: {contest_id})")
    logger.error(f"最后的错误: {last_error}")
    return False, f"重试次数已耗尽，最后错误: {last_error}"

def check_contest_completion(contest_output_dir, csv_filename):
    """
    检查比赛是否已经完全下载完成
    
    Args:
        contest_output_dir: 比赛输出目录
        csv_filename: CSV文件名
    
    Returns:
        tuple: (is_complete: bool, entry_count: int, valid_count: int)
    """
    contest_csv_path = os.path.join(contest_output_dir, f"Submission_Contestant_{csv_filename}.csv")
    
    if not os.path.exists(contest_csv_path):
        return False, 0, 0
    
    try:
        df = pd.read_csv(contest_csv_path)
        entry_count = len(df)
        
        if entry_count > 0:
            # 检查关键列是否存在
            required_columns = ['DesignID', 'Entry', 'UserID']
            if all(col in df.columns for col in required_columns):
                # 检查有效条目数量
                valid_entries = df[
                    df['DesignID'].notna() & 
                    (df['DesignID'] != 'N/A') & 
                    (df['DesignID'] != '')
                ]
                valid_count = len(valid_entries)
                
                if valid_count > 0:
                    return True, entry_count, valid_count
        
        return False, entry_count, 0
        
    except Exception as e:
        logger.error(f"检查CSV文件时出错: {contest_csv_path}, 错误: {e}")
        return False, 0, 0

def save_progress(progress_file, contest_id):
    """保存进度到文件"""
    try:
        with open(progress_file, 'a', encoding='utf-8') as f:
            f.write(f"{contest_id}\n")
    except Exception as e:
        logger.error(f"保存进度失败: {e}")

def load_progress(progress_file):
    """加载已完成的比赛ID列表"""
    completed = set()
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                completed = set(line.strip() for line in f if line.strip())
        except Exception as e:
            logger.error(f"加载进度文件失败: {e}")
    return completed

def main():
    # Set base path
    output_dir_list = "/Users/samxie/Research/CrowdDeleRej/Data/ContestList/"  # Path to save contest list CSV
    output_dir_Image = "/Users/samxie/Research/CrowdDeleRej/Data/ImageList/0828/"
    
    # Ensure output directories exist
    os.makedirs(output_dir_Image, exist_ok=True)  # Create directory to save images
    
    # Define path for the aggregated CSV file
    all_contests_csv = os.path.join(output_dir_Image, "ContestEntry_20250828.csv")  # Path for aggregated CSV file
    
    # 失败记录文件
    failed_contests_csv = os.path.join(output_dir_Image, "Failed_contests_entry.csv")
    
    # 进度记录文件
    progress_file = os.path.join(output_dir_Image, "crawl_progress.txt")
    
    logger.info(" 开始爬虫任务...")
    
    # Step 2: Load the contest data CSV file that was scraped
    contests_df_path = os.path.join(output_dir_list, "Contest_URL_All_0828.csv")  # Path to scraped results CSV
    contests_df = pd.read_csv(contests_df_path)
    contests_df = contests_df.astype(str)  # Convert all columns to string type
    
    total_contests = len(contests_df)
    logger.info(f" 总共需要处理 {total_contests} 个比赛")
    
    # If the aggregated CSV file already exists, load existing data; otherwise, create an empty DataFrame
    if os.path.exists(all_contests_csv):
        all_contests_df = pd.read_csv(all_contests_csv)
        logger.info(f" 加载了已存在的聚合文件，包含 {len(all_contests_df)} 条记录")
    else:
        all_contests_df = pd.DataFrame()
        logger.info(" 创建新的聚合DataFrame")
    
    # 加载失败记录
    failed_contests = []
    if os.path.exists(failed_contests_csv):
        failed_df = pd.read_csv(failed_contests_csv)
        logger.info(f"  加载了 {len(failed_df)} 条失败记录")
    else:
        failed_df = pd.DataFrame(columns=['ContestID', 'ContestName', 'ContestURL', 'FailureReason', 'AttemptTime'])
    
    # 加载进度记录
    completed_contests = load_progress(progress_file)
    logger.info(f" 已完成 {len(completed_contests)} 个比赛")
    
    # 统计信息
    successful_downloads = 0
    failed_downloads = 0
    skipped_downloads = 0
    
    # Step 4: Iterate through each contest and download images
    for index, row in contests_df.iterrows():
        contest_url = row['ContestURL']
        contest_id = row['ContestID']
        contest_name = row['ContestName']
        
        logger.info(f" 处理进度: {index + 1}/{total_contests} - {contest_name} (ID: {contest_id})")
        
        # 检查是否已经完成
        if contest_id in completed_contests:
            logger.info(f"⏭  比赛已完成，跳过: {contest_name} (ID: {contest_id})")
            skipped_downloads += 1
            continue
        
        # Create a specific directory for each contest to save images
        contest_output_dir = os.path.join(output_dir_Image, contest_id)  # Path to save each contest's images
        os.makedirs(contest_output_dir, exist_ok=True)
        
        # 检查是否已经有完整的数据
        csv_filename = f"{contest_id}_contest"  # Filename to save contest images CSV
        is_complete, entry_count, valid_count = check_contest_completion(contest_output_dir, csv_filename)
        
        if is_complete:
            logger.info(f" 数据已完整，直接合并: {contest_name} (ID: {contest_id}) - {entry_count} 条记录 ({valid_count} 有效)")
            
            # 直接加载并合并数据
            contest_csv_path = os.path.join(contest_output_dir, f"Submission_Contestant_{csv_filename}.csv")
            contest_df = pd.read_csv(contest_csv_path)
            all_contests_df = pd.concat([all_contests_df, contest_df], ignore_index=True)
            
            # 保存进度
            save_progress(progress_file, contest_id)
            successful_downloads += 1
            
            # 定期保存聚合数据
            if successful_downloads % 5 == 0:
                all_contests_df.to_csv(all_contests_csv, index=False)
                logger.info(f" 已保存中间进度，累计成功 {successful_downloads} 个比赛")
            
            continue
        
        # Step 5: Download images for the contest with retry mechanism
        logger.info(f"🔄 开始下载比赛: {contest_name} (ID: {contest_id})...")
        
        download_success, error_message = download_contest_with_retry(
            contest_url, 
            contest_output_dir, 
            csv_filename,
            contest_id,
            contest_name,
            max_retries=20,  # 最多重试20次
            base_delay=2     # 基础延迟3秒
        )
        
        if download_success:
            successful_downloads += 1
            logger.info(f" 比赛下载成功: {contest_name} (ID: {contest_id})")
            
            # Step 6: Load the image data CSV for the current contest and merge it into the aggregated DataFrame
            contest_csv_path = os.path.join(contest_output_dir, f"Submission_Contestant_{csv_filename}.csv")
            
            if os.path.exists(contest_csv_path):
                try:
                    contest_df = pd.read_csv(contest_csv_path)
                    all_contests_df = pd.concat([all_contests_df, contest_df], ignore_index=True)
                    
                    # 保存进度
                    save_progress(progress_file, contest_id)
                    
                    logger.info(f" 已合并数据: {len(contest_df)} 条记录")
                    
                    # 定期保存聚合数据，防止数据丢失
                    if successful_downloads % 5 == 0:
                        all_contests_df.to_csv(all_contests_csv, index=False)
                        logger.info(f" 已保存中间进度，累计成功 {successful_downloads} 个比赛")
                        
                except Exception as e:
                    logger.error(f"合并数据时出错: {contest_name} (ID: {contest_id}), 错误: {e}")
            
        else:
            failed_downloads += 1
            current_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 记录失败的比赛
            failed_contest_info = {
                'ContestID': contest_id,
                'ContestName': contest_name,
                'ContestURL': contest_url,
                'FailureReason': error_message,
                'AttemptTime': current_time
            }
            failed_contests.append(failed_contest_info)
            logger.error(f" 记录失败比赛: {contest_name} (ID: {contest_id}) - {error_message}")
        
        # 每处理10个比赛保存一次失败记录
        if len(failed_contests) >= 10:
            try:
                failed_df_temp = pd.DataFrame(failed_contests)
                failed_df = pd.concat([failed_df, failed_df_temp], ignore_index=True)
                failed_df.to_csv(failed_contests_csv, index=False)
                failed_contests = []  # 清空临时列表
                logger.info(f" 已保存失败记录，当前总失败数量: {failed_downloads}")
            except Exception as e:
                logger.error(f"保存失败记录时出错: {e}")
        
        # 添加请求间隔，避免过于频繁的请求
        time.sleep(random.uniform(1, 3))
    
    # 保存最终的失败记录
    if failed_contests:
        try:
            failed_df_temp = pd.DataFrame(failed_contests)
            failed_df = pd.concat([failed_df, failed_df_temp], ignore_index=True)
            failed_df.to_csv(failed_contests_csv, index=False)
            logger.info(f" 保存了最终的失败记录")
        except Exception as e:
            logger.error(f"保存最终失败记录时出错: {e}")
    
    # Step 7: Final save of the aggregated data and remove duplicates
    if not all_contests_df.empty:
        try:
            logger.info(" 正在处理最终数据...")
            
            # Remove duplicates based on DesignID
            original_count = len(all_contests_df)
            all_contests_df.drop_duplicates(subset='DesignID', keep='first', inplace=True)
            deduplicated_count = len(all_contests_df)
            
            if original_count > deduplicated_count:
                logger.info(f"🧹 去重完成: 移除了 {original_count - deduplicated_count} 条重复记录")
            
            # Sort by ContestID (descending) and Entry (descending within each ContestID)
            all_contests_df.sort_values(by=['ContestID', 'Entry'], ascending=[False, False], inplace=True)
            
            # 最终保存
            all_contests_df.to_csv(all_contests_csv, index=False)
            logger.info(f" 聚合CSV文件已保存: {all_contests_csv}")
            
        except Exception as e:
            logger.error(f"处理最终数据时出错: {e}")
    
    # 输出最终统计信息
    logger.info("="*60)
    logger.info(" 爬取任务完成统计:")
    logger.info(f" 总比赛数量: {total_contests}")
    logger.info(f" 成功下载: {successful_downloads}")
    logger.info(f" 失败下载: {failed_downloads}")
    logger.info(f"⏭  跳过下载: {skipped_downloads}")
    
    if total_contests > 0:
        success_rate = successful_downloads / total_contests * 100
        logger.info(f"🎯 成功率: {success_rate:.2f}%")
    
    if failed_downloads > 0:
        logger.info(f"📋 失败记录已保存到: {failed_contests_csv}")
    
    logger.info("="*60)
    print(f"Aggregated CSV file saved as {all_contests_csv}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info(" 用户中断了程序")
    except Exception as e:
        logger.error(f" 程序发生未预期的错误: {e}")
        import traceback
        logger.error(traceback.format_exc())