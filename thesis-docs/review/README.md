# Review Tugas Akhir — `Tugas-Akhir-Luthfan-10-Jul.pdf`

Dokumen ini adalah hasil review menyeluruh atas draf TA per 10 Juli 2026 (100 halaman).
Review mencakup lima sudut: (1) kepatuhan ke notulensi bimbingan, (2) sisa placeholder/konten
kedaluwarsa, (3) kecocokan listing kode dengan repo, (4) kecocokan angka dengan xlsx + Spark
event log, dan (5) bahasa/storytelling/standar penulisan TA ITS.

Ringkasan umum: **isi BAB 4 (hasil & pembahasan) dan BAB 5 sudah kuat, prosanya rapi dan
angkanya hampir seluruhnya cocok dengan data ground-truth.** Masalah terbesar bukan pada
analisis, melainkan pada **sisa catatan draf yang lolos ke PDF, beberapa listing kode yang salah/
usang, front matter yang belum di-regenerate, dan konten metodologi (BAB 3) yang masih
menyimpan rencana lama (SF50, "chunking", "cleaning")**.

## Daftar berkas review

| Berkas | Isi |
|--------|-----|
| [01-kepatuhan-notulensi.md](01-kepatuhan-notulensi.md) | Kepatuhan ke instruksi Pak Faizal (Big Data, kuantitatif, cleaning, satuan detik, dsb.) |
| [02-placeholder-dan-stale.md](02-placeholder-dan-stale.md) | Placeholder mentah, SF50 kedaluwarsa, front matter, cross-reference "Tabel 4.x" |
| [03-verifikasi-kode.md](03-verifikasi-kode.md) | Listing kode BAB 4 vs `src/` dan `dbt_transform/` |
| [04-verifikasi-data-angka.md](04-verifikasi-data-angka.md) | Angka di tabel vs xlsx + event log |
| [05-bahasa-dan-penulisan.md](05-bahasa-dan-penulisan.md) | Gaya bahasa, storytelling, standar penulisan, contoh perbaikan paragraf |

## Temuan paling kritis (harus diperbaiki sebelum sidang)

Diurutkan berdasarkan dampak. Detail dan lokasi ada di berkas masing-masing.

1. **Catatan draf mentah lolos ke PDF (2 tempat).**
   - Hal. 47: `(skema penjelasan ETL, dari ekstraksi, misal kayak kalo CSV pakai function apa … jelasin juga bahwa akan pakai chunking, chunknya berapa, dll)` — catatan untuk diri sendiri, bahasa non-formal.
   - Hal. 78 (4.3.3.3): `((Mau ditambahin grafik volume input per format))`.
   → **Wajib dihapus/ditulis ulang.** Fatal jika terbaca penguji. Lihat [02](02-placeholder-dan-stale.md).

2. **Listing kode `mode="overwrite"` untuk ELT & Hibrida — kode asli memakai `mode="append"`.**
   Kode 4.11 (hal. 54) dan Kode 4.27 (hal. 66) menampilkan `overwrite`, dan seluruh paragraf
   penjelas dibangun di atas klaim "overwrite memastikan tabel bersih tiap run". Faktanya
   `src/elt/elt_query9.py` dan `src/hybrid/hybrid_query9.py` memakai `append` — justru karena
   itulah tabel `raw.*` harus di-`TRUNCATE` manual antar-run. Klaim di tesis **terbalik**. Lihat [03](03-verifikasi-kode.md).

3. **DAFTAR ISI tidak memuat BAB 4 dan BAB 5.** Daftar isi melompat dari `3.2 … 39` langsung
   ke `DAFTAR PUSTAKA 92`. DAFTAR TABEL berhenti di Tabel 3.3, DAFTAR GAMBAR di Gambar 3.4,
   DAFTAR KODE di Kode 4.30 (padahal ada s.d. Kode 4.38). Front matter belum di-update. Lihat [02](02-placeholder-dan-stale.md).

4. **BAB 3 masih menyimpan rencana SF50 lama.** Subbab 3.1.8.1 menyebut *"Skala besar (SF 50,
   ±50 GB): lineitem ~300.060.750 baris, orders ~75.000.000"* dan *"setiap ukuran SF (10 dan 50)"*
   — padahal eksperimen nyata SF5 + SF10. Ini bertentangan dengan 3.1.3 (yang sudah benar SF5+SF10)
   dan seluruh BAB 4. Juga hal. 51: *"data berskala 50 GB"*. Lihat [02](02-placeholder-dan-stale.md).

5. **Tabel 4.16 & Subbab 4.3.2/4.4.2 memakai data sampler `ps` (CPU% / RSS) yang tidak andal.**
   Angka CPU 335%, RSS 2009 MB dst. berasal dari sampler proses yang—menurut analisis event-log
   penulis sendiri—tidak reliabel. Ini juga **berbenturan dengan BAB 5 poin 2** yang menyatakan
   kesimpulan sumber daya "berdasarkan metrik andal dari Spark event log" (shuffle/spill) dan sama
   sekali tidak menyebut CPU%. Perlu diputuskan: buang Tabel 4.16 dan tulis ulang 4.3.2/4.4.2 di
   atas shuffle/spill (konsisten dengan BAB 5), atau beri justifikasi eksplisit. Lihat [04](04-verifikasi-data-angka.md).

6. **"chunking" disebut 5× tapi tidak ada di kode.** Skrip Spark memakai `spark.read.load()` biasa;
   tidak ada pembacaan bertahap manual. "Chunking" adalah sisa rencana era pandas (sebelum
   REVISI 2 migrasi ke PySpark). Klaim ini keliru secara teknis. Lihat [03](03-verifikasi-kode.md).

7. **Rumusan Masalah menyebut "kuantitatif dan kualitatif".** Subbab 1.2: *"analisis komparatif
   secara kuantitatif dan kualitatif"*. Instruksi pembimbing: metrik harus **kuantitatif**. Studi ini
   memang kuantitatif (dan 3.1.1 sudah menulis "secara kuantitatif" saja). Hapus "dan kualitatif". Lihat [01](01-kepatuhan-notulensi.md).

## Temuan penting lain (ringkas)

- **Tabel 4.25 salah angka:** sel Hibrida–JSON–SF5 tertulis `193,19`, seharusnya `207,10`
  (rasio 2,27 = 469,87/207,10 membuktikannya). Lihat [04](04-verifikasi-data-angka.md).
- **Listing kode utama (Kode 4.7, 4.38) usang:** masih menampilkan `time.sleep(3600)` dan
  `FILE_FORMAT`/`"{file_type}"`, padahal kode kini memakai `KEEP_UI_OPEN` + `input()` +
  `FILE_FORMAT_RUN`. Lihat [03](03-verifikasi-kode.md).
- **Listing ekstraksi ETL & ELT (Kode 4.2, 4.9) hanya menampilkan cabang `csv`,** padahal
  judulnya "Multi-Format" dan prosanya membahas jsonl/parquet. (Hibrida Kode 4.24 sudah lengkap
  3 cabang — jadikan acuan.) Lihat [03](03-verifikasi-kode.md).
- **"Big Data" / framing 5V masih dominan** di Abstrak, Latar Belakang, dan Subbab 2.2.1
  ("Karakteristik dan Tantangan pada Big Data") — bertentangan dengan arahan pembimbing. Lihat [01](01-kepatuhan-notulensi.md).
- **Belum ada satu pun grafik di BAB 4.** Metodologi 3.1.9 menjanjikan "visualisasi dalam bentuk
  grafik" dan pembimbing (bimb 1) meminta hasil dijelaskan lewat grafik lebih dulu. Saat ini hasil
  hanya berupa tabel + 2 placeholder grafik. Lihat [02](02-placeholder-dan-stale.md) & [05](05-bahasa-dan-penulisan.md).
- **`Error! Bookmark not defined.`** di DAFTAR GAMBAR (Gambar 3.3). Lihat [02](02-placeholder-dan-stale.md).
- **Banyak cross-reference belum tersambung:** "Tabel 4.x", "tabel 4.15" (maksudnya 4.18),
  "tabel 4.12" untuk SF10 (maksudnya 4.13). Lihat [02](02-placeholder-dan-stale.md).
- **Skema tabel (4.3–4.10) tidak sepenuhnya cocok dengan `convert.py`:** tanggal ditulis
  `DATETIME` (kode: `DATE`), kunci `INTEGER` (kode: `BIGINT`), numerik `NUMERIC(12,2)`
  (kode: `DECIMAL(15,2)`), `l_quantity` `INTEGER` (kode: `DECIMAL(15,2)`). Lihat [03](03-verifikasi-kode.md).

## Yang sudah bagus (jangan diutak-atik)

- Angka BAB 4.3.1/4.4 (durasi total, per fase, penyimpanan, skalabilitas, rasio) **cocok** dengan
  `tpch_sf5/sf10_experiment_result_analysis` dan event log.
- Narasi 4.4 (analisis durasi, format, skalabilitas) dan BAB 5 sudah ditulis dengan register skripsi
  yang tepat — deklaratif, sebab-akibat, tidak "ngeblog".
- Temuan inti benar dan kuat: ETL < Hibrida < ELT konsisten di dua skala; bottleneck ELT/Hibrida
  di pemuatan JDBC (bukan transformasi); Parquet tercepat; JSONL terboros; pembalikan Parquet
  pada ELT di SF10; hanya ETL yang shuffle/spill.
- Dasar Teori (BAB 2) sudah menjelaskan TPC-H, dbt, Parquet secara rinci sesuai permintaan pembimbing.
