"""
데이터 로드 및 필터링 모듈
Streamlit 캐시를 활용하여 성능 최적화
"""
import pandas as pd
import streamlit as st
from pathlib import Path

# 경로 설정
BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data" / "processed" / "congestion_clean.parquet"


@st.cache_data
def load_data():
    """
    Parquet 데이터 로드 및 캐시
    
    Returns:
        pd.DataFrame: 정제된 혼잡도 데이터
    """
    df = pd.read_parquet(DATA_PATH)
    return df


@st.cache_data
def get_filter_options(df):
    """
    필터링에 사용할 옵션 추출
    
    Args:
        df: 원본 데이터프레임
        
    Returns:
        dict: 각 필터의 옵션 리스트
    """
    return {
        'weekdays': sorted(df['weekday'].unique().tolist()),
        'lines': sorted(df['line'].unique().tolist()),
        'stations': sorted(df['station_name'].unique().tolist()),
        'directions': sorted(df['direction'].unique().tolist()),
        'time_slots': sorted(df['time_slot'].unique().tolist(), 
                           key=lambda x: df[df['time_slot']==x]['time_order'].iloc[0])
    }


def filter_data(df, weekday=None, lines=None, stations=None, directions=None, time_range=None):
    """
    사이드바 필터 적용
    
    Args:
        df: 원본 데이터프레임
        weekday: 요일 선택 (문자열 또는 None)
        lines: 호선 선택 (리스트 또는 None)
        stations: 역 선택 (리스트 또는 None)
        directions: 방향 선택 (리스트 또는 None)
        time_range: 시간 범위 (튜플 (start_order, end_order) 또는 None)
        
    Returns:
        pd.DataFrame: 필터링된 데이터프레임
    """
    filtered = df.copy()
    
    # 요일 필터
    if weekday and weekday != '전체':
        filtered = filtered[filtered['weekday'] == weekday]
    
    # 호선 필터
    if lines and len(lines) > 0:
        filtered = filtered[filtered['line'].isin(lines)]
    
    # 역 필터
    if stations and len(stations) > 0:
        filtered = filtered[filtered['station_name'].isin(stations)]
    
    # 방향 필터
    if directions and len(directions) > 0:
        filtered = filtered[filtered['direction'].isin(directions)]
    
    # 시간 범위 필터
    if time_range:
        start_order, end_order = time_range
        filtered = filtered[
            (filtered['time_order'] >= start_order) & 
            (filtered['time_order'] <= end_order)
        ]
    
    return filtered


def get_time_order_mapping(df):
    """
    시간 슬롯과 time_order 매핑 생성
    
    Args:
        df: 원본 데이터프레임
        
    Returns:
        dict: {time_slot: time_order} 매핑
    """
    return df[['time_slot', 'time_order']].drop_duplicates().set_index('time_slot')['time_order'].to_dict()


def prepare_download_data(df):
    """
    다운로드용 데이터 준비 (결측 플래그 제거, 정렬)
    
    Args:
        df: 필터링된 데이터프레임
        
    Returns:
        pd.DataFrame: 다운로드용 데이터프레임
    """
    # 필요한 컬럼만 선택
    cols = ['weekday', 'line', 'station_name', 'direction', 'time_slot', 
            'congestion', 'hour', 'period']
    
    download_df = df[cols].copy()
    
    # 정렬
    download_df = download_df.sort_values(['line', 'station_name', 'weekday', 
                                           'direction', 'time_slot'])
    
    return download_df
