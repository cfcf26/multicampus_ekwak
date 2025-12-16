"""
서울교통공사 지하철 혼잡도 데이터 ETL 스크립트
원본 CSV를 대시보드용 롱 포맷 Parquet으로 변환
"""
import pandas as pd
import re
from pathlib import Path

# 경로 설정
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DATA_PATH = BASE_DIR / "data" / "processed" / "congestion_clean.parquet"

# CSV 파일 동적 탐색 (한글 파일명 문제 회피)
def find_csv_file():
    """data/raw 디렉토리에서 CSV 파일 찾기"""
    csv_files = list(RAW_DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"data/raw 디렉토리에 CSV 파일이 없습니다: {RAW_DATA_DIR}")
    return csv_files[0]

RAW_DATA_PATH = find_csv_file()


def load_csv(filepath):
    """CSV 로드 (인코딩 자동 감지)"""
    print(f"[CSV 로드] {filepath}")
    
    # 인코딩 시도 순서: cp949 → utf-8
    for encoding in ['cp949', 'utf-8']:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            print(f"[OK] 인코딩: {encoding}")
            print(f"[OK] 원본 데이터 shape: {df.shape}")
            return df
        except UnicodeDecodeError:
            continue
    
    raise ValueError("CSV 파일 인코딩을 인식할 수 없습니다.")


def get_time_columns(df):
    """시간 컬럼 추출 (5시30분 형태)"""
    time_pattern = re.compile(r'\d+시\d+분')
    time_cols = [col for col in df.columns if time_pattern.match(col)]
    print(f"[OK] 시간 컬럼 개수: {len(time_cols)}")
    return time_cols


def normalize_time_slot(time_str):
    """시간 문자열 정규화: '5시30분' → '05:30'"""
    match = re.match(r'(\d+)시(\d+)분', time_str)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        return f"{hour:02d}:{minute:02d}"
    return time_str


def create_time_order_map(time_cols):
    """시간 순서 매핑 생성 (정렬용)"""
    time_map = {}
    for idx, time_col in enumerate(time_cols):
        normalized = normalize_time_slot(time_col)
        time_map[time_col] = {'normalized': normalized, 'order': idx}
    return time_map


def unpivot_time_columns(df, time_cols):
    """시간 컬럼을 행으로 변환 (Wide → Long Format)"""
    print("[Unpivot] Wide -> Long Format 변환 중...")
    
    # ID 컬럼 (시간 컬럼 제외)
    id_cols = ['요일구분', '호선', '역번호', '출발역', '상하구분']
    
    # Melt 수행
    df_long = df.melt(
        id_vars=id_cols,
        value_vars=time_cols,
        var_name='time_slot_raw',
        value_name='congestion_raw'
    )
    
    print(f"[OK] 변환 후 shape: {df_long.shape}")
    return df_long


def clean_congestion_values(df):
    """혼잡도 값 정제 (공백 제거, float 변환)"""
    print("[정제] 혼잡도 값 정제 중...")
    
    # 공백 제거
    df['congestion_raw'] = df['congestion_raw'].astype(str).str.strip()
    
    # float 변환
    df['congestion'] = pd.to_numeric(df['congestion_raw'], errors='coerce')
    
    # NaN 개수 확인
    nan_count = df['congestion'].isna().sum()
    print(f"[WARNING] NaN 변환 실패: {nan_count}개")
    
    return df


def add_time_features(df, time_map):
    """시간 관련 파생 컬럼 추가"""
    print("[시간] 시간 파생 컬럼 생성 중...")
    
    # 정규화된 시간 및 순서
    df['time_slot'] = df['time_slot_raw'].map(lambda x: time_map[x]['normalized'])
    df['time_order'] = df['time_slot_raw'].map(lambda x: time_map[x]['order'])
    
    # 시간 추출
    df['hour'] = df['time_slot'].str.split(':').str[0].astype(int)
    
    # 시간대 구분
    def get_period(hour):
        if 0 <= hour < 1:
            return '심야'
        elif 5 <= hour < 7:
            return '새벽'
        elif 7 <= hour < 9:
            return '출근'
        elif 9 <= hour < 12:
            return '오전'
        elif 12 <= hour < 18:
            return '오후'
        elif 18 <= hour < 20:
            return '퇴근'
        elif 20 <= hour < 24:
            return '저녁'
        else:
            return '기타'
    
    df['period'] = df['hour'].apply(get_period)
    
    return df


def handle_missing_values(df):
    """0.0 및 결측값 처리"""
    print("[결측] 결측값 처리 중...")
    
    # 0.0 값 표시 (미운행 구간으로 추정)
    df['is_missing'] = (df['congestion'] == 0.0) | (df['congestion'].isna())
    
    missing_count = df['is_missing'].sum()
    missing_pct = (missing_count / len(df)) * 100
    print(f"[WARNING] 결측/0.0 값: {missing_count}개 ({missing_pct:.2f}%)")
    
    return df


def rename_columns(df):
    """컬럼명 영문화"""
    print("[컬럼] 컬럼명 변경 중...")
    
    df = df.rename(columns={
        '요일구분': 'weekday',
        '호선': 'line',
        '역번호': 'station_id',
        '출발역': 'station_name',
        '상하구분': 'direction'
    })
    
    return df


def validate_data(df):
    """데이터 검증"""
    print("\n" + "="*60)
    print("[검증] 데이터 검증 결과")
    print("="*60)
    
    # 1. 시간 슬롯 개수
    unique_time_slots = df['time_slot'].nunique()
    print(f"[OK] 시간 슬롯 개수: {unique_time_slots}")
    
    # 2. 호선 목록
    unique_lines = sorted(df['line'].unique())
    print(f"[OK] 호선 목록: {', '.join(unique_lines)}")
    
    # 3. 요일 목록
    unique_weekdays = df['weekday'].unique()
    print(f"[OK] 요일 목록: {', '.join(unique_weekdays)}")
    
    # 4. 역 개수
    unique_stations = df['station_name'].nunique()
    print(f"[OK] 역 개수: {unique_stations}개")
    
    # 5. Row count 일관성 (역/호선/요일/방향 조합별)
    group_counts = df.groupby(['weekday', 'line', 'station_name', 'direction']).size()
    if group_counts.nunique() == 1:
        print(f"[OK] 조합별 row count 일관성: OK (각 {group_counts.iloc[0]}개)")
    else:
        print(f"[WARNING] 조합별 row count 불일치: {group_counts.value_counts()}")
    
    # 6. 혼잡도 범위
    print(f"[OK] 혼잡도 범위: {df['congestion'].min():.1f} ~ {df['congestion'].max():.1f}")
    
    # 7. 100 초과값
    over_100 = (df['congestion'] > 100).sum()
    over_100_pct = (over_100 / len(df)) * 100
    print(f"[OK] 100 초과값: {over_100}개 ({over_100_pct:.2f}%)")
    
    # 8. 최종 shape
    print(f"[OK] 최종 데이터 shape: {df.shape}")
    print("="*60 + "\n")


def save_parquet(df, filepath):
    """Parquet 파일로 저장"""
    print(f"[저장] Parquet 저장 중: {filepath}")
    
    # 불필요한 컬럼 제거
    cols_to_keep = [
        'weekday', 'line', 'station_id', 'station_name', 'direction',
        'time_slot', 'time_order', 'congestion', 'hour', 'period', 'is_missing'
    ]
    df_final = df[cols_to_keep]
    
    # 정렬 (호선, 역, 요일, 방향, 시간 순)
    df_final = df_final.sort_values(['line', 'station_id', 'weekday', 'direction', 'time_order'])
    
    # 저장
    df_final.to_parquet(filepath, index=False, engine='pyarrow')
    
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    print(f"[OK] 저장 완료 (파일 크기: {file_size_mb:.2f} MB)")


def main():
    """메인 ETL 프로세스"""
    print("\n" + "="*60)
    print("[ETL] 서울교통공사 지하철 혼잡도 ETL 시작")
    print("="*60 + "\n")
    
    # 1. CSV 로드
    df = load_csv(RAW_DATA_PATH)
    
    # 2. 시간 컬럼 추출
    time_cols = get_time_columns(df)
    
    # 3. 시간 순서 매핑 생성
    time_map = create_time_order_map(time_cols)
    
    # 4. Unpivot
    df_long = unpivot_time_columns(df, time_cols)
    
    # 5. 혼잡도 값 정제
    df_long = clean_congestion_values(df_long)
    
    # 6. 시간 파생 컬럼 추가
    df_long = add_time_features(df_long, time_map)
    
    # 7. 결측값 처리
    df_long = handle_missing_values(df_long)
    
    # 8. 컬럼명 영문화
    df_long = rename_columns(df_long)
    
    # 9. 데이터 검증
    validate_data(df_long)
    
    # 10. Parquet 저장
    save_parquet(df_long, PROCESSED_DATA_PATH)
    
    print("\n" + "="*60)
    print("[완료] ETL 완료!")
    print("="*60 + "\n")


if __name__ == '__main__':
    main()
