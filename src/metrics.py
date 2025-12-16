"""
KPI 및 메트릭 계산 모듈
혼잡도 관련 통계 및 랭킹 기능 제공
"""
import pandas as pd
import numpy as np


def get_max_congestion_info(df):
    """
    최대 혼잡도 및 해당 정보 반환
    
    Args:
        df: 필터링된 데이터프레임
        
    Returns:
        dict: 최대 혼잡도 정보
            - max_value: 최대 혼잡도 값
            - time_slot: 발생 시간
            - station_name: 역명
            - line: 호선
            - direction: 방향
            - weekday: 요일
    """
    # 결측값 제외
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        return {
            'max_value': 0.0,
            'time_slot': '-',
            'station_name': '-',
            'line': '-',
            'direction': '-',
            'weekday': '-'
        }
    
    # 최대값 찾기
    max_idx = valid_df['congestion'].idxmax()
    max_row = valid_df.loc[max_idx]
    
    return {
        'max_value': max_row['congestion'],
        'time_slot': max_row['time_slot'],
        'station_name': max_row['station_name'],
        'line': max_row['line'],
        'direction': max_row['direction'],
        'weekday': max_row['weekday']
    }


def get_top_n_stations(df, n=10, time_range=None, aggregate='max'):
    """
    Top-N 혼잡 역 랭킹
    
    Args:
        df: 필터링된 데이터프레임
        n: 상위 N개
        time_range: 시간 범위 필터 (튜플 (start_order, end_order) 또는 None)
        aggregate: 집계 방식 ('max', 'mean', 'sum')
        
    Returns:
        pd.DataFrame: 역별 혼잡도 랭킹
            컬럼: station_name, line, congestion_value
    """
    # 결측값 제외
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        return pd.DataFrame(columns=['station_name', 'line', 'congestion_value'])
    
    # 시간 범위 필터
    if time_range:
        start_order, end_order = time_range
        valid_df = valid_df[
            (valid_df['time_order'] >= start_order) & 
            (valid_df['time_order'] <= end_order)
        ]
    
    # 역+호선별 집계
    if aggregate == 'max':
        ranking = valid_df.groupby(['station_name', 'line'])['congestion'].max()
    elif aggregate == 'mean':
        ranking = valid_df.groupby(['station_name', 'line'])['congestion'].mean()
    elif aggregate == 'sum':
        ranking = valid_df.groupby(['station_name', 'line'])['congestion'].sum()
    else:
        ranking = valid_df.groupby(['station_name', 'line'])['congestion'].max()
    
    # 상위 N개
    top_n = ranking.nlargest(n).reset_index()
    top_n.columns = ['station_name', 'line', 'congestion_value']
    
    return top_n


def get_average_congestion_by_period(df):
    """
    시간대별 평균 혼잡도
    
    Args:
        df: 필터링된 데이터프레임
        
    Returns:
        pd.DataFrame: 시간대별 평균 혼잡도
    """
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        return pd.DataFrame(columns=['period', 'avg_congestion'])
    
    period_avg = valid_df.groupby('period')['congestion'].mean().reset_index()
    period_avg.columns = ['period', 'avg_congestion']
    
    # 시간대 순서 정렬
    period_order = ['새벽', '출근', '오전', '오후', '퇴근', '저녁', '심야']
    period_avg['period'] = pd.Categorical(period_avg['period'], categories=period_order, ordered=True)
    period_avg = period_avg.sort_values('period')
    
    return period_avg


def get_congestion_stats(df):
    """
    혼잡도 통계 요약
    
    Args:
        df: 필터링된 데이터프레임
        
    Returns:
        dict: 통계 정보
            - count: 데이터 개수
            - mean: 평균
            - median: 중앙값
            - std: 표준편차
            - min: 최소값
            - max: 최대값
            - missing_pct: 결측 비율
    """
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        return {
            'count': 0,
            'mean': 0.0,
            'median': 0.0,
            'std': 0.0,
            'min': 0.0,
            'max': 0.0,
            'missing_pct': 100.0
        }
    
    congestion = valid_df['congestion']
    missing_pct = (df['is_missing'].sum() / len(df)) * 100
    
    return {
        'count': len(valid_df),
        'mean': congestion.mean(),
        'median': congestion.median(),
        'std': congestion.std(),
        'min': congestion.min(),
        'max': congestion.max(),
        'missing_pct': missing_pct
    }


def get_peak_hours(df, threshold=None):
    """
    피크 시간대 찾기
    
    Args:
        df: 필터링된 데이터프레임
        threshold: 혼잡도 임계값 (None이면 평균 + 1*std 사용)
        
    Returns:
        pd.DataFrame: 피크 시간대 정보
    """
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        return pd.DataFrame(columns=['time_slot', 'avg_congestion', 'max_congestion'])
    
    # 시간대별 통계
    time_stats = valid_df.groupby('time_slot').agg({
        'congestion': ['mean', 'max'],
        'time_order': 'first'
    }).reset_index()
    
    time_stats.columns = ['time_slot', 'avg_congestion', 'max_congestion', 'time_order']
    
    # 임계값 설정
    if threshold is None:
        threshold = valid_df['congestion'].mean() + valid_df['congestion'].std()
    
    # 피크 시간대 필터링
    peak_hours = time_stats[time_stats['avg_congestion'] >= threshold].copy()
    peak_hours = peak_hours.sort_values('time_order')
    
    return peak_hours[['time_slot', 'avg_congestion', 'max_congestion']]
