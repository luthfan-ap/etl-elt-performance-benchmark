# 02 — Placeholder Mentah, Konten Kedaluwarsa, & Front Matter

Bagian ini mengumpulkan semua yang **belum dikerjakan / lupa dibersihkan**: catatan draf yang
lolos, sisa rencana SF50, front matter yang belum di-regenerate, dan cross-reference yang belum
tersambung.

## A. Catatan draf mentah yang lolos ke PDF (WAJIB dihapus)

### A1. Placeholder deskripsi skema ETL — hal. 47 (Subbab 4.2.1)
Teks berikut tercetak apa adanya di badan tulisan, tepat sebelum 4.2.1.1:

> *(skema penjelasan ETL, dari ekstraksi, misal kayak kalo CSV pakai function apa, kalo JSONL
> pakai function apa, kalo parquet pakai function apa, dll. jelasin juga bahwa akan pakai chunking,
> chunknya berapa, dll)*

- Bahasa non-formal ("misal kayak kalo"), jelas catatan untuk diri sendiri.
- Menyebut **chunking** yang tidak ada di kode (lihat [03](03-verifikasi-kode.md)).
- **Tindakan:** hapus. Bila memang ingin ada paragraf pengantar arsitektur ETL, tulis 2–3 kalimat
  deklaratif: ETL membaca 6 tabel via PySpark (CSV pakai `header=True`, JSONL via reader `json`,
  Parquet langsung), lalu seluruh join/filter/agregasi Query 9 dijalankan di memori Spark sebelum
  hanya 175 baris hasil dimuat ke PostgreSQL.

### A2. Placeholder grafik — hal. 78 (akhir Subbab 4.3.3.3)

> *((Mau ditambahin grafik volume input per format))*

- **Tindakan:** buat grafiknya (atau hapus catatannya). Terkait poin C di bawah (BAB 4 belum
  punya grafik sama sekali).

## B. Sisa rencana SF50 (eksperimen nyata = SF5 + SF10)

Draf awal merencanakan SF10 + SF50 di laptop Windows. Realisasi: SF5 + SF10 di MacBook Air M3.
Sebagian teks sudah diperbarui, tetapi beberapa masih menyimpan SF50:

| Lokasi | Teks bermasalah | Perbaikan |
|--------|-----------------|-----------|
| **3.1.8.1** (hal. 37) | *"untuk setiap ukuran SF (10 dan 50)"* dan *"Skala besar (SF 50, ±50 GB): lineitem ~300.060.750 baris, orders ~75.000.000"* | Ganti ke SF5 & SF10; skala kecil = SF5 (~5 GB, lineitem ~30 jt, orders ~7,5 jt), skala besar = SF10 (~10 GB, lineitem ~60 jt, orders ~15 jt) |
| **4.2.1.3** (hal. 51) | *"seluruh beban kerja komputasi data berskala 50 GB dieksekusi…"* | Ganti "50 GB" → deskripsi umum atau "hingga 10 GB (SF10)" |
| Saran BAB 5 poin 1 | *"skala yang jauh lebih besar (misalnya SF50 dan seterusnya)"* | ✅ **Ini boleh tetap** — konteksnya saran future work, bukan klaim yang dikerjakan |

**Inkonsistensi internal BAB 3 yang perlu dirapikan:** 3.1.3 sudah benar (SF5+SF10), tapi 3.1.4.1
hanya membahas SF10 (Tabel 3.2 = SF=10, tanpa tabel SF5), sementara 3.1.8.1 memakai SF10+SF50.
Samakan seluruh BAB 3 ke narasi **SF5 + SF10**. Idealnya tampilkan **dua** tabel perkiraan ukuran
(SF5 dan SF10), atau satu tabel SF5 dengan catatan "SF10 ≈ 2× SF5".

## C. BAB 4 belum memiliki grafik

- Metodologi **3.1.9** menjanjikan *"visualisasi dalam bentuk grafik untuk menunjukkan total durasi
  pemrosesan dan penggunaan sumber daya"*.
- Pembimbing (bimb 1) meminta di sidang **hasil dijelaskan lewat grafik lebih dulu**, detail teknis
  belakangan.
- Kondisi sekarang: 4.3–4.4 seluruhnya tabel; ada 2 catatan "mau ditambah grafik".
- **Tindakan (disarankan minimal):**
  1. Grafik batang **total waktu per skenario** (9 skenario, SF5) — visualisasi Tabel 4.12.
  2. Grafik **stacked bar per fase** (Spark vs dbt) untuk ELT & Hibrida — visualisasi Tabel 4.14.
  3. Grafik **rasio pertumbuhan SF5→SF10** — visualisasi Tabel 4.25.
  4. (Opsional) grafik **volume input per format** (yang diminta di catatan hal. 78).

## D. Front matter belum di-regenerate (kritis untuk kelengkapan buku TA)

| Bagian | Masalah | Perbaikan |
|--------|---------|-----------|
| **DAFTAR ISI** | Melompat dari `3.2 … 39` ke `DAFTAR PUSTAKA 92`. **BAB 4 HASIL DAN PEMBAHASAN dan BAB 5 KESIMPULAN DAN SARAN tidak ada.** | Regenerate TOC; masukkan seluruh subbab 4.1–4.4.x dan 5.1–5.2 |
| **DAFTAR TABEL** | Berhenti di Tabel 3.3. Tabel 4.1–4.26 (25+ tabel) tidak terdaftar. Juga: judul "Tabel 3.2 … SF = 50" (badan sudah SF=10), dan "Tabel 3.3 Jadwal" (badan: Tabel 3.3 = Skenario, Tabel 3.4 = Jadwal → nomor bergeser) | Regenerate; perbaiki nomor & judul Tabel 3.2/3.3/3.4 |
| **DAFTAR GAMBAR** | Berhenti di Gambar 3.4. Ada `Gambar 3.3 … Error! Bookmark not defined.` | Perbaiki bookmark Gambar 3.3; regenerate |
| **DAFTAR KODE** | Berhenti di Kode 4.30. Kode 4.31–4.38 tidak terdaftar | Regenerate |

## E. Cross-reference belum tersambung ("Tabel 4.x" & nomor salah)

Banyak referensi silang masih placeholder atau salah nomor. Yang terdeteksi:

| Lokasi | Tertulis | Seharusnya |
|--------|----------|------------|
| 4.1.3 (hal. 44) | "dua kali lipat dari nilai pada **Tabel 4.x**" | Tabel 4.11 |
| 4.3.1.1 (hal. 73) | SF10 "disajikan pada **tabel 4.12**" | Tabel 4.13 |
| 4.3.3.2 (hal. 77) | "Hasil pada **tabel 4.15** mengkuantifikasi…" | Tabel 4.18 |
| 4.4.2 (hal. 82) | "diringkas pada **Tabel 4.x**" | Tabel 4.21 |
| 4.4.3.1 (hal. 83) | "disajikan pada **Tabel 4.x**" | Tabel 4.22 |
| 4.4.4.1 (hal. 86) | "tampak dari **Tabel 4.x**" | Tabel 4.25 |

Lakukan pass menyeluruh mengganti semua "Tabel 4.x"/"Tabel di bawah ini" menjadi nomor pasti,
dan pakai fitur cross-reference Word agar tidak bergeser lagi.

## F. Kalimat "menunggu SF10" yang sudah kedaluwarsa

SF10 sudah selesai dan tabelnya sudah ada di BAB 4, tapi beberapa kalimat masih bernada "SF10
belum tersedia / akan diuji":

- **4.4 intro** (hal. 79): *"analisis skalabilitas yang **akan dilengkapi setelah eksperimen SF10
  tersedia** (4.4.4)"* → ganti: *"analisis skalabilitas berdasarkan perbandingan SF5 dan SF10 (4.4.4)"*.
- **4.4.2** (hal. 82): *"ketahanannya terhadap peningkatan skala perlu diperhatikan, dan hal ini
  **akan diuji pada eksperimen SF10**"* → ganti ke bentuk lampau: *"…dan hal ini telah diuji pada
  SF10 (Subbab 4.4.4), di mana ETL tetap menyelesaikan seluruh skenario tanpa kegagalan memori."*

## G. Judul subbab ganda / salah

- **4.3.3.2 dan 4.3.3.3 berjudul sama**: keduanya "Ukuran Tabel di Data Warehouse". Padahal
  4.3.3.3 sebenarnya berisi **metrik I/O internal Spark (shuffle/spill/peak execution memory)**,
  bukan ukuran tabel. Ganti judul 4.3.3.3 → **"Metrik I/O Internal Spark"** (atau serupa).

## H. Ketidakkonsistenan minor lain

- **Tabel 4.18:** typo **"partsuppp"** (tiga p) → "partsupp".
- **Tabel 4.18:** total ELT ditulis "~7.292 MB (7,3 GB)" di tabel tapi "sekitar 7,1 GB" di paragraf;
  Hibrida "2.451 MB (2,5 GB)" vs "2,4 GB". Beda pembulatan ÷1000 vs ÷1024. Samakan satu konvensi.
- **Paragraf 4.3.3.2:** "hibrida hanya **1,954 MB**" — koma di sini terbaca ~2 MB (notasi Indonesia).
  Maksudnya **1.954 MB**. Samakan pemisah ribuan (titik) di seluruh dokumen.
- **Abstrak** hanya menyebut metrik "waktu pemrosesan total dan efisiensi sumber daya (CPU dan
  memori)" — tidak menyebut **penyimpanan** dan **skalabilitas**, padahal itu RQ3 & RQ4 dan
  dibahas penuh di BAB 4. Tambahkan agar abstrak mencakup keempat rumusan masalah.
