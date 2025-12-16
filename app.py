"""
서울교통공사 지하철 혼잡도 대시보드
Streamlit MVP
"""
import streamlit as st
import pandas as pd
from pathlib import Path

# 로컬 모듈 임포트
from src.data import (
    load_data, 
    prepare_download_data,
    get_filter_options,
    get_time_order_mapping,
    filter_data
)
from src.metrics import (
    get_max_congestion_info,
    get_top_n_stations,
    get_congestion_stats
)
from src.charts import (
    create_heatmap,
    create_line_chart,
    create_ranking_bar,
    create_time_distribution
)


# 페이지 설정
st.set_page_config(
    page_title="서울 지하철 혼잡도 대시보드",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded"
)


def main():
    """메인 앱"""
    
    # 타이틀
    st.title("🚇 서울교통공사 지하철 혼잡도 대시보드")
    st.markdown("---")
    
    # 데이터 로드
    with st.spinner("데이터 로딩 중..."):
        df = load_data()
        filter_options = get_filter_options(df)
        time_order_map = get_time_order_mapping(df)
    
    # 사이드바 필터
    st.sidebar.header("🔍 필터 설정")
    
    # 요일 선택
    weekday_options = ['전체'] + filter_options['weekdays']
    selected_weekday = st.sidebar.selectbox(
        "요일 선택",
        weekday_options,
        index=0
    )
    
    # 호선 선택 (다중 선택)
    selected_lines = st.sidebar.multiselect(
        "호선 선택 (다중 선택 가능)",
        filter_options['lines'],
        default=filter_options['lines']  # 기본값: 전체 선택
    )
    
    # 역 선택 (다중 선택, 검색 가능)
    selected_stations = st.sidebar.multiselect(
        "역 선택 (검색 가능, 다중 선택 가능)",
        filter_options['stations'],
        default=[],  # 기본값: 빈 리스트 (전체)
        help="역을 선택하지 않으면 전체 역이 표시됩니다"
    )
    
    # 방향 선택 (다중 선택)
    selected_directions = st.sidebar.multiselect(
        "방향 선택 (다중 선택 가능)",
        filter_options['directions'],
        default=filter_options['directions']  # 기본값: 전체 선택
    )
    
    # 시간 범위 선택
    st.sidebar.markdown("### 시간 범위 선택")
    time_slots = filter_options['time_slots']
    
    # 슬라이더용 인덱스
    min_idx = 0
    max_idx = len(time_slots) - 1
    
    time_range_idx = st.sidebar.slider(
        "시간대 범위",
        min_value=min_idx,
        max_value=max_idx,
        value=(min_idx, max_idx),
        format=""
    )
    
    # 선택된 시간 표시
    start_time = time_slots[time_range_idx[0]]
    end_time = time_slots[time_range_idx[1]]
    st.sidebar.info(f"선택된 시간: **{start_time}** ~ **{end_time}**")
    
    # time_order로 변환
    start_order = time_order_map[start_time]
    end_order = time_order_map[end_time]
    time_range = (start_order, end_order)
    
    # 필터 적용
    filtered_df = filter_data(
        df,
        weekday=selected_weekday,
        lines=selected_lines,
        stations=selected_stations if len(selected_stations) > 0 else None,
        directions=selected_directions,
        time_range=time_range
    )
    
    # 데이터 개수 표시
    st.sidebar.markdown("---")
    st.sidebar.metric("필터링된 데이터", f"{len(filtered_df):,}건")
    
    # 메인 영역
    if len(filtered_df) == 0:
        st.warning("⚠️ 선택한 필터 조건에 해당하는 데이터가 없습니다. 필터를 조정해주세요.")
        return
    
    # KPI 카드
    st.header("📊 주요 지표 (KPI)")
    
    max_info = get_max_congestion_info(filtered_df)
    stats = get_congestion_stats(filtered_df)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="최대 혼잡도",
            value=f"{max_info['max_value']:.1f}",
            help="선택된 조건에서 가장 높은 혼잡도 값"
        )
    
    with col2:
        st.metric(
            label="평균 혼잡도",
            value=f"{stats['mean']:.1f}",
            help="선택된 조건의 평균 혼잡도"
        )
    
    with col3:
        st.metric(
            label="발생 시간",
            value=max_info['time_slot'],
            help="최대 혼잡도가 발생한 시간대"
        )
    
    with col4:
        st.metric(
            label="발생 역",
            value=max_info['station_name'],
            help=f"{max_info['line']} {max_info['direction']}"
        )
    
    # 최대 혼잡도 상세 정보
    with st.expander("🔍 최대 혼잡도 상세 정보"):
        st.markdown(f"""
        - **역명**: {max_info['station_name']}
        - **호선**: {max_info['line']}
        - **방향**: {max_info['direction']}
        - **요일**: {max_info['weekday']}
        - **시간**: {max_info['time_slot']}
        - **혼잡도**: {max_info['max_value']:.1f}
        """)
    
    st.markdown("---")
    
    # 차트 섹션
    st.header("📈 시각화")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔥 히트맵", 
        "📉 시간대별 추이", 
        "🏆 Top-N 랭킹",
        "📦 분포"
    ])
    
    with tab1:
        st.subheader("역별 시간대 혼잡도 히트맵")
        st.markdown("혼잡도가 높은 상위 역의 시간대별 패턴을 한눈에 확인할 수 있습니다.")
        
        # 히트맵 설정
        col_heat1, col_heat2 = st.columns([3, 1])
        with col_heat2:
            max_stations = st.slider(
                "표시할 역 개수",
                min_value=5,
                max_value=50,
                value=20,
                step=5,
                key="heatmap_stations"
            )
        
        heatmap_fig = create_heatmap(filtered_df, max_stations=max_stations)
        st.plotly_chart(heatmap_fig, use_container_width=True)
    
    with tab2:
        st.subheader("시간대별 혼잡도 추이")
        st.markdown("선택한 역의 시간대별 혼잡도 변화를 확인할 수 있습니다.")
        
        # 역 선택 (라인 차트용)
        col_line1, col_line2 = st.columns([3, 1])
        with col_line1:
            line_chart_stations = st.multiselect(
                "추이를 확인할 역 선택 (최대 5개 권장)",
                options=filter_options['stations'],
                default=[] if len(selected_stations) == 0 else selected_stations[:5],
                key="line_chart_stations",
                help="역을 선택하지 않으면 평균 혼잡도가 높은 상위 5개 역이 표시됩니다"
            )
        
        line_fig = create_line_chart(
            filtered_df, 
            selected_stations=line_chart_stations if len(line_chart_stations) > 0 else None
        )
        st.plotly_chart(line_fig, use_container_width=True)
    
    with tab3:
        st.subheader("혼잡도 Top-N 랭킹")
        st.markdown("혼잡도가 가장 높은 역을 랭킹으로 확인할 수 있습니다.")
        
        # 랭킹 설정
        col_rank1, col_rank2, col_rank3 = st.columns([2, 1, 1])
        with col_rank2:
            top_n = st.slider(
                "표시할 순위",
                min_value=5,
                max_value=30,
                value=10,
                step=5,
                key="ranking_n"
            )
        with col_rank3:
            agg_method = st.selectbox(
                "집계 방식",
                ["max", "mean"],
                format_func=lambda x: "최대값" if x == "max" else "평균값",
                key="ranking_agg"
            )
        
        ranking_fig = create_ranking_bar(filtered_df, n=top_n, aggregate=agg_method)
        st.plotly_chart(ranking_fig, use_container_width=True)
    
    with tab4:
        st.subheader("시간대별 혼잡도 분포")
        st.markdown("각 시간대의 혼잡도 분포(박스플롯)를 확인할 수 있습니다.")
        
        dist_fig = create_time_distribution(filtered_df)
        st.plotly_chart(dist_fig, use_container_width=True)
    
    st.markdown("---")
    
    # 데이터 테이블 및 다운로드
    st.header("📋 데이터 미리보기 및 다운로드")
    
    col_table1, col_table2 = st.columns([3, 1])
    
    with col_table1:
        show_rows = st.slider(
            "표시할 행 수",
            min_value=10,
            max_value=500,
            value=100,
            step=10,
            key="table_rows"
        )
    
    with col_table2:
        # CSV 다운로드
        download_df = prepare_download_data(filtered_df)
        csv_data = download_df.to_csv(index=False, encoding='utf-8-sig')
        
        st.download_button(
            label="📥 CSV 다운로드",
            data=csv_data,
            file_name=f"혼잡도_데이터_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            help="현재 필터링된 데이터를 CSV 파일로 다운로드합니다"
        )
    
    # 테이블 표시
    st.dataframe(
        filtered_df[['weekday', 'line', 'station_name', 'direction', 
                     'time_slot', 'congestion', 'period']].head(show_rows),
        use_container_width=True,
        hide_index=True
    )
    
    st.info(f"💡 총 {len(filtered_df):,}건 중 {show_rows}건을 표시하고 있습니다.")
    
    # 푸터
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray;'>
        <small>서울교통공사 지하철 혼잡도 정보 (2025년 9월 30일 기준)</small><br>
        <small>💡 Tip: 사이드바에서 필터를 조정하여 원하는 조건의 데이터를 탐색하세요</small>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
