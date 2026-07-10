# Draft Tugas Akhir (Markdown)

Struktur folder per bab/subbab. **Isi tiap file ditulis sebagai teks final siap salin-tempel (copy-paste) ke dokumen TA.**

## Cara pakai

- **Badan teks** (paragraf & tabel biasa) = siap ditempel apa adanya ke Word.
- **Blok kutipan `>`** = **catatan dari asisten / bagian yang belum final** (data yang belum ada, spesifikasi yang masih bisa berubah, TODO, hal yang perlu diperiksa). **Jangan ditempel** ke dokumen; hapus setelah ditindaklanjuti.
- **Penomoran** `Tabel 4.x` / `Kode 4.x` sengaja dibiarkan sebagai placeholder — ganti dengan nomor berjalan sesuai urutan di Word (find-replace).
- Format angka memakai **koma desimal** (mis. `12,81`) sesuai konvensi Bahasa Indonesia. Ubah ke titik bila dokumenmu memakai titik.
- Angka hasil eksperimen berasal dari rekap pengukuran penulis: **SF5** dari `thesis-docs/tpch_sf5_experiment_result_analysis.xlsx` dan **SF10** dari `logs/metrics/scenario_times.csv` (keduanya 3 run, MacBook Air M3). Nilai = rata-rata 3 run ± STDEV.S.

## Status BAB 4 (HASIL DAN PEMBAHASAN)

| Subbab | Isi | Status |
|--------|-----|--------|
| 4.1 Persiapan Lingkungan dan Data | lingkungan uji + akuisisi/konversi data | **teks siap tempel** (cek catatan `>`: spec laptop & versi software) |
| 4.2 Implementasi Arsitektur | narasi ETL / ELT / Hibrida | **teks siap tempel**; sisipkan potongan Kode 4.x dari `src/` & `dbt_transform/` di titik `>` |
| 4.3 Hasil Pengujian | pengukuran waktu, sumber daya, penyimpanan | **teks siap tempel** (SF5 + SF10). Metrik sumber daya (4.3.2) memakai Spark event log yang andal (SF5+SF10); sisi PostgreSQL tidak terinstrumentasi (keterbatasan) |
| 4.4 Pembahasan | analisis perbandingan + interpretasi | **teks siap tempel** (SF5 + SF10); **4.4.4 Skalabilitas LENGKAP** (waktu + penyimpanan) |

## Status BAB 5 (PENUTUP)

| Subbab | Isi | Status |
|--------|-----|--------|
| 5.1 Kesimpulan | menjawab 4 rumusan masalah | **teks siap tempel & LENGKAP** — RQ1–RQ4 semua terisi (waktu, sumber daya, penyimpanan warehouse, skalabilitas SF5+SF10) |
| 5.2 Saran | saran penelitian lanjutan | **teks siap tempel** (butir SF10 & warehouse sudah direvisi jadi tercapai) |

## Catatan skala data

Eksperimen **SF5 dan SF10** sudah selesai (keduanya di MacBook Air M3), sehingga analisis skalabilitas (4.4.4 dan butir kesimpulan RQ4) sudah ditulis penuh. Metrik sumber daya (4.3.2/4.4.2) memakai **Spark event log** yang andal untuk SF5 dan SF10 (menggantikan sampler `ps` yang terbukti tidak andal); yang belum terinstrumentasi hanya sisi PostgreSQL selama transformasi ELT/Hibrida, dan itu dinyatakan sebagai keterbatasan. Ukuran tabel warehouse sudah diukur untuk SF5 dan SF10.
