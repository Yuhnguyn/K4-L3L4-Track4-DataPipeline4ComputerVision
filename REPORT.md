# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`).

| | |
|---|---|
| Họ và tên | Nguyễn Phúc Huy |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602911 |
| GitHub | Yuhnguyn |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-06570 |

## Kết quả cuối

- `dataset_version` của bạn: `ea27d4d3511e`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **693 / 160 / 166** (tổng 1019 khung, 26 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (`27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_crash_graph.png`, `airflow_crash_log.png`.

---

## Bước 1 · Bronze

### Câu 1
Bốn khác biệt giữa telemetry A và B ở bronze: (1) **tên cột** khác nhau — A dùng `speed_mps`, `ts_utc`, `gas`/`brake`, `lat`/`lon`/`alt_m`; B dùng `spd_kmh`, `timestamp_ms`, `throttle_pct`/`brake_pct`, `latitude`/`longitude`/`altitude`. (2) **Đơn vị tốc độ**: A là m/s (1.213), B là km/h (55.11). (3) **Kiểu thời gian**: A là chuỗi ISO (`2025-10-07T08:00:00.014Z`), B là epoch mili-giây (`1759933801509`). (4) **Bàn đạp**: A ghi phân số (0.028), B ghi phần trăm (2.0); ngoài ra B còn khai báo **độ lệch đồng hồ 1500 ms** (A là 0). Vì mỗi nguồn có schema khác nhau nên phải **mỗi nguồn một bảng bronze riêng** — gộp sớm sẽ phải ép các cột không khớp vào nhau và làm mất cột đặc thù. Mọi cột để dạng **chữ** để bronze giữ **nguyên bản trung thực, bất biến**: không diễn giải, không đoán kiểu, không làm mất giá trị gốc; việc chuẩn hoá/ép kiểu và quyết định "đúng/sai" được để cho tầng silver. Nếu ép kiểu ngay ở bronze, một ô bẩn có thể làm vỡ cả batch và ta mất dữ liệu gốc để truy vết.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline `union all by name` hai bảng staging thành một schema chung: thời gian `ts` (timestamptz UTC), tốc độ tính bằng **m/s**, hướng độ, toạ độ, gia tốc, `gas`/`brake` (phân số), góc lái. Nguồn B được chuẩn hoá và **trừ độ lệch đồng hồ**: `to_timestamp((timestamp_ms - offset_ms)/1000)`, `spd_kmh/3.6`, `throttle_pct/100`; độ lệch 1500 ms lấy từ `bronze.video_files`. Bằng chứng: sau chuẩn hoá, thời điểm bắt đầu của B là `2025-10-08 14:30:00.009` **khớp** thời điểm bắt đầu video, và tốc độ tối đa hợp lý (100 km/h < 130). Bảng quarantine mỗi drive: **duplicate 20 + speed_missing 30 + speed_out_of_range 3 = 53**, và **sạch + cách ly = nhận về** (2968+53=3021 ở A; 3181+53=3234 ở B). Dòng lỗi **không bị xoá im lặng** mà vào `silver.silver_telemetry_rejects` kèm `reject_reason`: giữ khả năng audit, phân biệt lỗi nguồn với lỗi hệ thống và cho phép tái xử lý sau này.

### Câu 3
Test `max_gap_ms` là **`warn`** vì khoảng trống tín hiệu cảm biến là hiện tượng **bình thường, có thể chấp nhận** (mất sóng GPS, cảm biến dropout) — chỉ cần báo để biết, không nên dừng cả pipeline. Test `in_range` là **`error`** vì tốc độ âm hoặc lớn hơn 70 m/s (252 km/h) là **bất khả thi về vật lý** — chắc chắn là dữ liệu sai, phải chặn để không đầu độc tầng gold. Thí nghiệm `break_gate`: nới `max_speed_mps` từ 70 lên **1000** m/s làm giá trị vô lý lọt vào silver, `dbt test` chuyển sang `ERROR=1` và bắt được **6 dòng** ở `in_range_silver_telemetry_speed_mps__70__0`; khôi phục luật gốc thì `ERROR=0`. Điều này cho thấy **cổng chất lượng** thật sự hoạt động: khi luật làm sạch bị nới sai, cổng chặn dữ liệu xấu **trước khi** nó đi tiếp tới gold.

## Bước 3 · Silver frames

### Câu 4
`idempotency.json`: trước khi chạy lại bảng có **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau khi chạy lại partition `trip-00003` **hai lần** vẫn **1246 dòng**, dấu vân tay `6c7480dfde7e` → **không đổi**. Trong khi đó cách ghi sai kiểu `append` chạy 2 lần: **120 dòng** cho một trip chỉ có 60 khung → **nhân đôi (2x)**. Delta `overwrite` theo **partition (`predicate trip_id`)** an toàn hơn xoá-rồi-ghi vì nó là **một commit nguyên tử**: hoặc toàn bộ partition được thay, hoặc (nếu lỗi giữa chừng) không có commit nào và bảng vẫn ở version cũ. Cách xoá-rồi-ghi lại có **khoảng trống giữa hai bước**; nếu task sập ngay sau khi xoá thì partition bị mất/hỏng. Vì vậy chạy lại một trip sau lỗi luôn cho **đúng một bản** của trip đó, không nhân đôi và không mất dữ liệu.

### Câu 5
**Khung đen** (brightness < `dark_luma` = 12) gần như **không có thông tin** nên **bị loại**. **Khung tối** (12 ≤ brightness < `dark_soft` = 40) là đường hầm, bóng râm, ban đêm — **điều kiện thật của đường**, nên được **gắn cờ `is_dark` nhưng giữ lại**; tôi đồng ý vì loại chúng sẽ khiến mô hình không bao giờ học được cảnh tối, còn giữ chúng lại giúp mô hình bền vững hơn. Ngưỡng dHash (số bit được phép khác tối đa) — số khung bị coi là trùng:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Pipeline dùng ngưỡng **2 bit**: đủ để bắt các khung gần như trùng (camera đứng hình, xe bò) mà không loại nhầm khung khác nhau thật. Ngưỡng này **phụ thuộc tốc độ xe**: xe chạy chậm (drive A có đoạn đứng hình) khiến các khung liền nhau gần giống nhau → dHash lệch ít → bị coi là trùng nhiều (A = 149); xe chạy nhanh hơn (drive B) khung thay đổi nhiều → ít trùng hơn (B = 53). Chọn ngưỡng cao quá sẽ xoá cả dữ liệu thật, thấp quá thì để lại nhiều khung trùng.

## Bước 4 · Gold

### Câu 6
`leakage.json`: 1019 khung thuộc 26 trip. Chia **theo khung hình** (ngẫu nhiên 70/15/15) làm **26 trip bị chia cắt** và **26 trip có khung ở cả train lẫn test**; chia **theo trip** (gold của pipeline) thì **0 trip bị chia cắt**. Điểm kiểm thử bị "đẹp giả tạo" vì các **khung liền nhau gần như giống hệt nhau** (một cảnh quay liên tục). Nếu nửa khung của cùng một chuyến nằm ở train và nửa kia ở test, thì khi huấn luyện mô hình **đã "thấy" gần đúng ảnh test** — độ chính xác test cao nhưng **không phản ánh khả năng tổng quát hoá** ra cảnh/chuyến xe mới. Vì vậy phải chia **theo trip (cảnh)**, không theo khung hình.

### Câu 7
`dataset_version` là **băm sha256 của danh sách `(frame_id | split)`** — tức là băm **nội dung** (khung nào vào split nào), **không** phụ thuộc ngày giờ chạy hay số đếm, nên hai lần chạy liên tiếp với cùng dữ liệu/split cho **cùng version** (`ea27d4d3511e`) — có thể dùng làm định danh tái lập. Version của tôi **khác bạn bè** dù cùng bộ dữ liệu vì **MSSV được trộn vào cách xếp trip**: trong `gold_dataset_frames` thứ tự trip là `md5(trip_id || LAB_STUDENT_ID)`, nên đổi MSSV → thứ tự chia khác → mỗi trip rơi vào split khác → danh sách `(frame|split)` khác → băm khác. (Khi thử đổi MSSV, `dataset_version` đổi; đặt lại MSSV của tôi thì ra lại `ea27d4d3511e`.)

### Câu 8
Phần `lineage` trong `manifest.json` của dataset tôi ghi rõ nó được tạo từ: các **version** của bảng nguồn — `bronze.raw_telemetry_a` (v0), `bronze.raw_telemetry_b` (v0), `bronze.trips_raw` (v0), `bronze.video_files` (v1), `silver.frames` (v27), `silver.telemetry` (v0), `silver.frames_flagged` (v0), `silver.frame_telemetry` (v0) — cùng **sha256 của hai video đầu vào** (`drive_a` = `636ba1a9…e9aa`, `drive_b` = `bfa62703…9fdc`), `dataset_version` = `ea27d4d3511e` và `manifest_sha256` = `6f633b69…d696`, kèm luật `drop` (`is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`) và luật chia `per trip, stratified by drive`. Nếu 3 tháng sau ai hỏi "dataset này từ đâu", ta dùng **`dataset_version` + sha256 của hai video + version các bảng** để **tái tạo đúng** dataset đó.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Với `LAB_FAIL_ONCE=trip-06570`, đúng **một** task `silver_frames` (map_index 17, tương ứng `trip-06570`) chuyển **đỏ**, log ghi `RuntimeError: simulated crash while extracting trip-06570; run it again`; **7 task** phía sau (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) chuyển `upstream_failed` vì cổng phụ thuộc task đó. Tôi **clear đúng task đỏ** (`silver_frames`, kèm downstream); Airflow chạy lại **chỉ partition `trip-06570`** rồi downstream, DAG chuyển **xanh**. Dữ liệu **không nhân đôi** vì `extract` dùng Delta **overwrite theo partition `trip_id`** (một trip = một partition), và `dataset_version` **không đổi** (`ea27d4d3511e`). Bằng chứng: `airflow_crash_graph.png` (1 task đỏ + downstream vàng), `airflow_crash_log.png` (log thấy `trip-06570`) và `airflow_graph.png` (DAG xanh sau khi clear).

### Câu 10
Nâng lên **500 xe/ngày**, ba thành phần vỡ đầu tiên: (1) **Airflow một máy** với `LocalExecutor`, chạy `airflow scheduler` + webserver trong **một container** — nghẽn và không chịu được tải; thay bằng **CeleryExecutor/KubernetesExecutor** với nhiều worker và autoscaling. (2) **Giải mã video bằng FFmpeg cục bộ** là nút cổ chai (decode là CPU-bound, mỗi trip một task tuần tự) — slide 24; thay bằng **decode bằng GPU / batch**, hoặc đẩy decode ra hàng đợi worker chuyên dụng. (3) **MinIO single-node + DuckDB + Delta trên cùng một máy** (storage và compute dính nhau) — thay bằng **object storage phân tán (S3) + warehouse/engine tách rời** (Spark/Trino/BigQuery, dbt chạy trên engine phân tán), slide 22–23. Ngoài ra khối lượng metadata/lineage và chi phí lưu ảnh cũng cần chính sách vòng đời dữ liệu.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL tôi thêm vào dashboard: tỷ lệ khung bị loại theo từng trip, để tìm trip "xấu" cần xem lại nguồn video:

```sql
select drive_id, trip_id, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped, split
from marts.mart_trip_summary
order by pct_dropped desc
```

Nó cho biết **trip nào có tỷ lệ khung bị loại cao nhất** (cứng camera: đen/mờ/trùng) và trip đó rơi vào split nào — dùng để kiểm tra chất lượng cảnh trước khi đưa vào huấn luyện.

## Nhìn lại

Điều bất ngờ nhất: tính **idempotent** đến từ một lựa chọn thiết kế rất nhỏ — ghi Delta `overwrite` theo `predicate trip_id` — mà biến việc "một task sập rồi chạy lại" từ đáng sợ thành vô hại; và `dataset_version` là **băm nội dung** nên chạy lại không đổi nhưng đổi MSSV thì đổi. Bài học vận hành: trên máy yếu, component dễ "chết" nhất không phải pipeline mà là **webserver** (gunicorn `No response from master within 120s`) — tôi phải nâng timeout mới ổn. Một lỗi dữ liệu tôi nghĩ pipeline **chưa** bắt được: khung **đứng hình** được phát hiện gián tiếp qua dHash (coi là gần trùng), nên nếu camera đứng hình khi cảnh vốn tối hoặc ít chi tiết thì có thể bị bỏ sót; và việc phát hiện trùng dựa vào **khung liền trước trong cùng trip**, nên hai khung giống nhau không liền kề vẫn có thể lọt. Ngoài ra, `purge_lake` xoá trong lúc đang phân trang có thể sót object (tôi gặp thực tế) — một lỗi vận hành đáng lẽ cần test.
