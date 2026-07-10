# 03 — Verifikasi Listing Kode BAB 4 vs Repo

Setiap listing Kode 4.x dibandingkan dengan `src/etl/etl_query9.py`, `src/elt/elt_query9.py`,
`src/hybrid/hybrid_query9.py`, `dbt_transform/models/**`, dan `dataset/convert.py`.

## ❌ KRITIS 1 — `mode="overwrite"` vs `mode="append"` (ELT & Hibrida)

**Kode 4.11 (ELT, hal. 54)** dan **Kode 4.27 (Hibrida, hal. 66)** menampilkan:

```python
df.write.jdbc(url=DB_URL, table=f"raw.elt_{table_name}", mode="overwrite", ...)
```

**Kode nyata** (`elt_query9.py:95`, `hybrid_query9.py:137`) memakai:

```python
mode="append"
```

Ini bukan sekadar beda satu kata: **seluruh paragraf penjelas dibangun di atas klaim yang salah.**
Kode 4.11 & 4.27 diikuti narasi seperti:

> *"Parameter mode='overwrite' digunakan agar tabel tujuan ditulis ulang setiap kali pengujian
> dijalankan … memastikan setiap skenario dimulai dari kondisi data yang bersih…"*

Faktanya, justru **karena memakai `append`**, tabel `raw.elt_*` / `raw.hybrid_*` **harus di-TRUNCATE
manual** antar-run (kalau tidak, baris menumpuk dan hasil/timing rusak). Ini detail metodologis yang
penting dan **saat ini terbalik di tesis**.

**Tindakan (pilih satu, tapi konsisten dengan kode):**
- Perbaiki listing → `mode="append"`, lalu jelaskan dengan benar: data mentah dimuat secara append,
  dan **sebelum tiap run tabel raw dikosongkan** (`TRUNCATE ... RESTART IDENTITY CASCADE`) untuk
  menjaga kebersihan pengukuran. Ini sekaligus menegaskan disiplin cold-start yang sudah diklaim di 4.3.1.
- (Catatan: **ETL** memang `mode="overwrite"`, jadi Kode 4.6 sudah benar — jangan ikut diubah.)

## ❌ KRITIS 2 — "chunking" tidak ada di kode

Disebut 5× (placeholder hal. 47; 4.1.1 hal. 42 "pembacaan data secara bertahap (chunking)"; 3.1.5,
3.1.6, 3.1.7 "secara bertahap (chunking)").

**Kode nyata:** ekstraksi hanya `spark.read.format(...).load(...)` — tidak ada iterasi chunk, tidak ada
parameter ukuran chunk. Spark mengelola paralelisme lewat partisi secara internal; tidak ada
"pembacaan bertahap" manual. "Chunking" adalah sisa rencana era **pandas** (sebelum REVISI 2
migrasi ke PySpark).

**Tindakan:** hapus semua penyebutan "chunking". Bila ingin menjelaskan manajemen memori, tulis
yang benar: Spark memproses data per-partisi secara *lazy* dan menumpahkan (*spill*) ke disk bila
memori kurang — itulah mekanisme nyata yang bahkan terlihat di metrik spill BAB 4.3.3.

## ❌ KRITIS 3 — Listing utama masih versi lama (`time.sleep(3600)`)

**Kode 4.7 (fungsi utama ETL, hal. 52)** dan **Kode 4.38 (fungsi utama Hibrida, hal. 71)** menampilkan:

```python
extract_data(table, "{file_type}")        # Kode 4.7 — template belum terisi
...
print("... Press CTRL+C to terminate ...")
time.sleep(3600)
spark.stop()
```

**Kode nyata** (`etl_query9.py:176-194`, `hybrid_query9.py:144-162`) sudah memakai:

```python
extracted_dfs[table] = extract_data(table, FILE_FORMAT_RUN)   # bukan "{file_type}"
...
KEEP_UI_OPEN = os.getenv("KEEP_UI_OPEN", "false").lower() in ("true","1","yes")
if KEEP_UI_OPEN:
    input("Press Enter to stop SparkSession...")
spark.stop()
```

Masalah ikutan: teks hal. 72 **menjelaskan panjang lebar** `time.sleep(3600)` ("untuk
mempertahankan Spark Web UI …") — padahal mekanisme itu sudah diganti `KEEP_UI_OPEN` + `input()`.
Kode 4.38 juga memakai `FILE_FORMAT` (variabel yang tidak ada; yang benar `FILE_FORMAT_RUN`).

**Tindakan:** ganti Kode 4.7 & 4.38 dengan versi terkini, dan ganti penjelasan `time.sleep` menjadi
penjelasan `KEEP_UI_OPEN`. Ini juga lebih bagus diceritakan (UI ditahan **hanya bila** flag aktif,
sehingga tidak mengganggu run batch).

## ⚠️ 4 — Listing ekstraksi ETL & ELT hanya menampilkan cabang CSV

- **Kode 4.2 (ETL, hal. 47)** dan **Kode 4.9 (ELT, hal. 53)** hanya memuat blok `if file_format=="csv"`,
  lalu langsung ke Kode 4.3/4.10 (`else: raise ValueError`). Cabang **jsonl** (special-case
  `.format("json")`) dan **parquet** hilang — padahal judul listing "Multi-Format" dan prosanya
  membahas ketiganya, termasuk keistimewaan jsonl.
- **Kode 4.24 (Hibrida, hal. 63)** sudah menampilkan **ketiga** cabang dengan benar → jadikan pola acuan.

**Tindakan:** lengkapi Kode 4.2 dan 4.9 dengan 3 cabang (csv/jsonl/parquet) seperti Kode 4.24.
Special-case JSONL (`.format("json")` untuk file `.jsonl`) adalah poin teknis menarik yang justru
layak ditonjolkan, bukan disembunyikan.

## ⚠️ 5 — Skema tabel (4.3–4.10) tidak sepenuhnya cocok dengan `convert.py`

`dataset/convert.py` mendefinisikan skema DuckDB eksplisit. Beda dengan tabel skema di 4.1.2:

| Aspek | Tesis (Tabel 4.3–4.10) | `convert.py` | Catatan |
|-------|------------------------|--------------|---------|
| Tanggal (`o_orderdate`, `l_shipdate`, dst.) | `DATETIME` | `DATE` | `DATE` yang benar (sesuai TPC-H & arahan format YYYY-MM-DD). Ganti label tesis → `DATE` |
| Kunci (`*_key`) | `INTEGER` | `BIGINT` | Data mentah = BIGINT; dbt staging baru cast ke INTEGER |
| Numerik (harga/biaya) | `NUMERIC(12,2)` | `DECIMAL(15,2)` | Presisi beda (12 vs 15) |
| `l_quantity` | `INTEGER` | `DECIMAL(15,2)` | Beda tipe |

Tidak fatal (dbt/Spark tetap cast saat dipakai), tapi karena tabel skema mengklaim "sesuai
spesifikasi TPC-H", sebaiknya diselaraskan dengan skema yang benar-benar dipakai `convert.py`
(atau dengan spesifikasi TPC-H resmi bila itu acuannya — tetapi harus konsisten).

## ⚠️ 6 — `sources.yml` memuat 8 tabel ELT, tapi hanya 6 yang dipakai

- **Kode 4.14** (dan `sources.yml` asli) mendaftarkan `elt_region` & `elt_customer` selain 6 tabel
  Query 9. **Tetapi** pipeline ELT (`elt_query9.py`) **hanya memuat 6 tabel** (nation, part, supplier,
  partsupp, orders, lineitem) dan **tidak ada** model `stg_elt_region`/`stg_elt_customer`.
- Prosa 4.2.2 (hal. 52) menulis *"lapisan staging … dari delapan tabel data mentah … lineitem, orders,
  part, partsupp, customer, supplier, nation, dan region"* — menyesatkan; hanya 6 yang benar-benar
  distaging (Kode 4.13 pun hanya 6 direktori model).

**Tindakan:** samakan narasi ke **6 tabel** yang relevan Query 9. Untuk `sources.yml`, boleh: (a)
hapus `elt_region`/`elt_customer` agar bersih, atau (b) beri catatan "region & customer didaftarkan
untuk kelengkapan tetapi tidak dipakai Query 9". Konsistenkan angka "delapan" → "enam".

## ✅ Listing yang sudah cocok dengan kode

- **Kode 4.1, 4.4, 4.5** (ETL job group, inisiasi tabel, transformasi Query 9) — **sama persis** dengan
  `etl_query9.py:60-152`. Bagus.
- **Kode 4.6** (ETL load, `mode="overwrite"`) — benar sesuai kode ETL.
- **Kode 4.8, 4.12** (ELT job group, koneksi `.env`) — cocok.
- **Kode 4.15–4.21** (model dbt ELT staging + mart) — cocok dengan `dbt_transform/models/elt/**`.
- **Kode 4.23–4.26** (Hibrida extract multi-format + clean select/cast) — cocok dengan
  `hybrid_query9.py:60-127`.
- **Kode 4.29–4.35** (model dbt Hibrida) — cocok dengan `dbt_transform/models/hybrid/**`.
- **Kode 4.13, 4.28** (struktur direktori dbt) — cocok (6 model staging + 1 mart per arsitektur).

## Catatan konseptual kecil

- **3.1.5** menyebut pemuatan ETL "menerapkan **SCD 1** (Update/Overwrite)". Untuk tabel **hasil**
  (bukan tabel dimensi), istilah SCD (Slowly Changing Dimension) kurang tepat — di sini hanya
  `mode="overwrite"` biasa. Cukup sebut "overwrite" tanpa melabeli SCD, agar tidak ditanya penguji.
- **4.2.1.2** menyebut penempatan filter di awal sebagai "predicate pushdown". Di Spark, pushdown
  dilakukan Catalyst optimizer terlepas dari urutan penulisan; yang dilakukan di kode lebih tepat
  disebut "early filtering". Nuansa kecil, boleh diperhalus ("mendorong filter sedini mungkin").
- **4.2.1.2** menyebut urutan join "mulai dari tabel kecil (supplier, nation) dulu, baru lineitem".
  Di kode, `lineitem` justru di-join lebih awal (dengan `part_filtered`) sebelum `supplier_nation`.
  Sesuaikan deskripsi urutan join dengan urutan aktual di Kode 4.5, atau lunakkan jadi "join disusun
  agar hasil antara tetap kecil".
