## 영화진흥위원회 데이터 파이프라인 구축 
- movie_summary.py DAG 생성 
- type 적용 - apply_type TASK
- 4개 df 합치기(merge) merge_df TASK 생성
- 합친 df 중복제거 de_dup TASK 생성
- summary 데이터 생성 summary_df TASK 생성

