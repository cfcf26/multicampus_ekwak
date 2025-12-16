"""
Plotly 차트 생성 모듈
히트맵, 라인차트, 랭킹 바차트 등 시각화 기능 제공
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np


def create_heatmap(df, max_stations=30):
    """
    역 x 시간대 혼잡도 히트맵
    
    Args:
        df: 필터링된 데이터프레임
        max_stations: 표시할 최대 역 개수 (너무 많으면 가독성 저하)
        
    Returns:
        plotly.graph_objects.Figure: 히트맵 차트
    """
    # 결측값 제외
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="데이터가 없습니다",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        return fig
    
    # 역 + 호선 조합으로 라벨 생성
    valid_df['station_label'] = valid_df['station_name'] + ' (' + valid_df['line'] + ')'
    
    # 평균 혼잡도가 높은 상위 N개 역만 선택
    top_stations = valid_df.groupby('station_label')['congestion'].mean().nlargest(max_stations).index
    plot_df = valid_df[valid_df['station_label'].isin(top_stations)].copy()
    
    # 피벗 테이블 생성
    pivot = plot_df.pivot_table(
        index='station_label',
        columns='time_slot',
        values='congestion',
        aggfunc='mean'
    )
    
    # time_order로 정렬
    time_order_map = valid_df[['time_slot', 'time_order']].drop_duplicates().set_index('time_slot')['time_order'].to_dict()
    sorted_columns = sorted(pivot.columns, key=lambda x: time_order_map.get(x, 999))
    pivot = pivot[sorted_columns]
    
    # 평균 혼잡도로 역 정렬
    pivot = pivot.loc[pivot.mean(axis=1).sort_values(ascending=False).index]
    
    # 히트맵 생성
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='RdYlGn_r',  # 빨강(높음) -> 노랑 -> 초록(낮음)
        colorbar=dict(title="혼잡도"),
        hovertemplate='역: %{y}<br>시간: %{x}<br>혼잡도: %{z:.1f}<extra></extra>'
    ))
    
    fig.update_layout(
        title=f"역별 시간대 혼잡도 히트맵 (상위 {len(pivot)}개 역)",
        xaxis_title="시간대",
        yaxis_title="역 (호선)",
        height=max(400, len(pivot) * 20),  # 역 개수에 따라 높이 조정
        xaxis=dict(tickangle=-45),
        font=dict(size=10)
    )
    
    return fig


def create_line_chart(df, selected_stations=None):
    """
    특정 역의 시간대별 혼잡도 라인 차트
    
    Args:
        df: 필터링된 데이터프레임
        selected_stations: 선택한 역 리스트 (None이면 상위 5개 역)
        
    Returns:
        plotly.graph_objects.Figure: 라인 차트
    """
    # 결측값 제외
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="데이터가 없습니다",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        return fig
    
    # 역 선택
    if selected_stations is None or len(selected_stations) == 0:
        # 평균 혼잡도 상위 5개 역
        top_5 = valid_df.groupby('station_name')['congestion'].mean().nlargest(5).index.tolist()
        plot_df = valid_df[valid_df['station_name'].isin(top_5)].copy()
    else:
        plot_df = valid_df[valid_df['station_name'].isin(selected_stations)].copy()
    
    if len(plot_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="선택한 역의 데이터가 없습니다",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        return fig
    
    # 역+방향+호선 조합으로 라벨 생성
    plot_df['line_label'] = (plot_df['station_name'] + ' (' + 
                              plot_df['line'] + ', ' + 
                              plot_df['direction'] + ')')
    
    # 시간대별 평균
    time_series = plot_df.groupby(['line_label', 'time_slot', 'time_order'])['congestion'].mean().reset_index()
    time_series = time_series.sort_values('time_order')
    
    # 라인 차트 생성
    fig = px.line(
        time_series,
        x='time_slot',
        y='congestion',
        color='line_label',
        markers=True,
        labels={'time_slot': '시간대', 'congestion': '혼잡도', 'line_label': '역 (호선, 방향)'},
        title='시간대별 혼잡도 추이'
    )
    
    fig.update_layout(
        hovermode='x unified',
        xaxis=dict(tickangle=-45),
        height=500,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    fig.update_traces(hovertemplate='혼잡도: %{y:.1f}')
    
    return fig


def create_ranking_bar(df, n=10, aggregate='max'):
    """
    Top-N 혼잡 역 랭킹 바 차트
    
    Args:
        df: 필터링된 데이터프레임
        n: 상위 N개
        aggregate: 집계 방식 ('max', 'mean')
        
    Returns:
        plotly.graph_objects.Figure: 바 차트
    """
    # 결측값 제외
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="데이터가 없습니다",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        return fig
    
    # 역+호선별 집계
    valid_df['station_label'] = valid_df['station_name'] + ' (' + valid_df['line'] + ')'
    
    if aggregate == 'max':
        ranking = valid_df.groupby('station_label')['congestion'].max()
        agg_label = '최대'
    elif aggregate == 'mean':
        ranking = valid_df.groupby('station_label')['congestion'].mean()
        agg_label = '평균'
    else:
        ranking = valid_df.groupby('station_label')['congestion'].max()
        agg_label = '최대'
    
    # 상위 N개
    top_n = ranking.nlargest(n).reset_index()
    top_n.columns = ['station_label', 'congestion_value']
    
    # 오름차순으로 정렬 (바 차트는 아래에서 위로)
    top_n = top_n.sort_values('congestion_value', ascending=True)
    
    # 색상 스케일 (높을수록 빨강)
    colors = top_n['congestion_value'].values
    
    # 바 차트 생성
    fig = go.Figure(data=go.Bar(
        x=top_n['congestion_value'],
        y=top_n['station_label'],
        orientation='h',
        marker=dict(
            color=colors,
            colorscale='RdYlGn_r',
            showscale=True,
            colorbar=dict(title="혼잡도")
        ),
        text=top_n['congestion_value'].round(1),
        textposition='outside',
        hovertemplate='역: %{y}<br>혼잡도: %{x:.1f}<extra></extra>'
    ))
    
    fig.update_layout(
        title=f'Top {n} 혼잡 역 ({agg_label} 혼잡도 기준)',
        xaxis_title='혼잡도',
        yaxis_title='역 (호선)',
        height=max(400, n * 40),
        showlegend=False
    )
    
    return fig


def create_time_distribution(df):
    """
    시간대별 혼잡도 분포 박스플롯
    
    Args:
        df: 필터링된 데이터프레임
        
    Returns:
        plotly.graph_objects.Figure: 박스플롯
    """
    valid_df = df[~df['is_missing']].copy()
    
    if len(valid_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="데이터가 없습니다",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20)
        )
        return fig
    
    # 시간대 순서대로 정렬
    valid_df = valid_df.sort_values('time_order')
    
    fig = px.box(
        valid_df,
        x='time_slot',
        y='congestion',
        labels={'time_slot': '시간대', 'congestion': '혼잡도'},
        title='시간대별 혼잡도 분포'
    )
    
    fig.update_layout(
        xaxis=dict(tickangle=-45),
        height=500
    )
    
    return fig
