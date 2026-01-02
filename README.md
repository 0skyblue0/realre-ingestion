# realre-ingestion

데이터 수집/처리용 인게이션 매니저.

## 실행 예시
- 한 번만 실행: `python Ingestion_Manager.py --schedule schedules/interval_schedule.json --once`
- 상시 스케줄링: `python Ingestion_Manager.py --schedule schedules/interval_schedule.json --poll 5`
- 비동기 실행 옵션: `--async`
- DB 경로 변경: `--db path/to/ingestion.db`

## 스케줄 JSON 스키마
```json
{
  "schema": "1.0",
  "jobs": [
    {
      "name": "job_name",
      "schedule": {"type": "interval", "seconds": 300},
      "args": {"source": "mock", "limit": 3}
    }
  ]
}
```
지원 타입: `interval`(초 단위), `daily`(`time: HH:MM`), `weekly`(`weekday`, `time`).
예시: `schedules/interval_schedule.json`, `schedules/weekly_schedule.json`.

## 검증 시나리오
1. `python Ingestion_Manager.py --schedule schedules/interval_schedule.json --once`
2. `realre_ingestion.db`가 생성되고 `ingestion_history`, `transactions_scd` 테이블에 레코드가 적재되는지 확인
3. 반복 실행 시 `transactions_scd`의 SCD2 히스토리가 적절히 버전업되는지 확인
