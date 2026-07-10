# 04 — Verifikasi Angka vs xlsx + Spark Event Log

Sumber ground-truth: `tpch_sf5_experiment_result_analysis_FIXED.xlsx`,
`tpch_sf10_experiment_result_analysis.xlsx` (keduanya dibangun ulang dari Spark event log
`logs/spark-logs/`), dan catatan hasil terverifikasi. Angka BAB 4 **sebagian besar cocok**.

## ✅ Angka yang cocok (aman)

| Tabel | Isi | Status |
|-------|-----|--------|
| Tabel 4.12 | Total waktu SF5: ETL 32,49/57,01/12,81 · ELT 408,26/451,88/382,33 · Hibrida 193,19/207,10/149,60 | ✅ cocok |
| Tabel 4.13 | Total waktu SF10: ETL 59,08/111,17/23,05 · ELT 825,46/952,22/926,87 · Hibrida 414,40/469,87/334,89 | ✅ cocok |
| Tabel 4.14 | Rincian fase SF5 (Spark + dbt = total); mis. ELT-CSV 259,77+148,48=408,26 | ✅ konsisten |
| Tabel 4.15 | Rincian fase SF10 | ✅ konsisten |
| Tabel 4.17 | Volume input Spark: CSV 5.327 MB / 42.549.826 baris; Parquet ETL&Hibrida 590 vs ELT 1.563 MB | ✅ cocok |
| Tabel 4.18 | Ukuran tabel DW SF5: lineitem elt 5.280 vs hybrid 1.954 MB; total ~7,3 vs ~2,5 GB | ✅ cocok |
| Tabel 4.19 | Shuffle/spill SF5: hanya ETL >0; ETL-CSV spill 48 MB mem + 18 MB disk; ELT/Hibrida 0 | ✅ cocok |
| Tabel 4.22–4.24 | Persentase Parquet/JSON vs CSV (60,6% / 75,5% / dst.) | ✅ cocok |
| Tabel 4.26 | Penyimpanan SF5 vs SF10: ELT ~7,1→13,3 GB (2,00×), ETL ~7,1→13,3... (lihat catatan) | ⚠️ lihat §Ralat 2 |
| BAB 5 | Spill CSV 66 MB (SF5) → 1.598 MB (SF10); Hibrida 49,8–63,9% lebih cepat dari ELT | ✅ cocok |

Konsistensi output 175 baris di seluruh skenario juga benar dan dinyatakan dengan baik di 4.3.

## ❌ Ralat 1 — Tabel 4.25, sel Hibrida–JSON–SF5

Tabel 4.25 (perbandingan SF5/SF10 + rasio):

```
Hibrida  CSV      193,19   414,40   2,15
Hibrida  JSON     193,19   469,87   2,27   ← SF5 salah
Hibrida  Parquet  149,60   334,89   2,24
```

Sel **SF5 untuk Hibrida–JSON tertulis `193,19`**, menyalin nilai baris CSV. Nilai benar = **`207,10`**
(lihat Tabel 4.12/4.14). Bukti: rasio yang tercetak `2,27` = 469,87 / **207,10**, bukan / 193,19
(yang akan memberi 2,43). Jadi rasionya sudah benar, hanya angka SF5-nya yang salah ketik.

**Tindakan:** ganti `193,19` → `207,10`.

## ❌ Ralat 2 — Tabel 4.26 baris "ETL" & "Hibrida" tertukar/salah

Tabel 4.26 saat ini:

```
Arsitektur   Tabel Mentah SF5   Tabel Mentah SF10   Rasio
ETL          ~7,1 GB            ~13,3 GB            1,87
ELT          ~2,4 GB            ~4,8 GB             2,00
Hibrida      Hanya 175 baris    Hanya 175 baris     -
```

Ini **tertukar**. Faktanya (Tabel 4.18 & narasi 4.3.3.2/4.4.4.3):
- **ELT** = tabel mentah terbesar (~7,1 GB SF5 → ~13,3 GB SF10).
- **Hibrida** = ~2,4 GB SF5 → ~4,8 GB SF10.
- **ETL** = tidak memuat tabel mentah sama sekali (hanya 175 baris hasil).

Jadi baris seharusnya:

```
ETL      Hanya 175 baris hasil   Hanya 175 baris hasil   -
ELT      ~7,1 GB                 ~13,3 GB                1,87
Hibrida  ~2,4 GB                 ~4,8 GB                 2,00
```

(rasio 1,87 milik ELT, 2,00 milik Hibrida — sesuai catatan hasil). **Tindakan:** susun ulang baris
Tabel 4.26 seperti di atas. Ini penting karena bertentangan langsung dengan paragraf tepat di
bawahnya yang sudah menyebut "tabel mentah ELT 7,1 GB … Hibrida 2,4 GB … ETL tidak
meninggalkan jejak".

## ❌ KRITIS — Tabel 4.16 & 4.3.2/4.4.2 memakai metrik sampler `ps` yang tidak andal

**Tabel 4.16** (CPU rata-rata/puncak %, RSS puncak MB) berasal dari **sampler proses `ps` tiap 2 detik**,
bukan dari event log. Berdasarkan analisis penulis sendiri saat membangun ulang xlsx dari event log,
sampler ini **tidak reliabel**: ia merata-ratakan sampling kasar dan memecah nama proses ber-spasi
sehingga kolomnya bergeser. Angka seperti "CPU 335,3% / RSS 2009 MB", "ELT extract_load RSS
816,9", "dbt_transform CPU 1,4%" berasal dari sumber ini.

**Bukti benturan internal:** **BAB 5 poin 2** menyatakan kesimpulan sumber daya
*"Berdasarkan metrik andal dari Spark event log"* dan **hanya** memakai **shuffle & spill** (nol pada
ELT/Hibrida) — **tidak menyebut CPU% sama sekali**. Artinya keputusan akhir penulis memang
membuang CPU%/RSS. Namun **BAB 4.3.2 (Tabel 4.16) dan 4.4.2 (serta "Bukti Pendukung" di
Tabel 4.21) masih menonjolkan CPU%/RSS** sebagai bukti utama. BAB 4 dan BAB 5 jadi tidak
sinkron.

**Tindakan (disarankan):**
1. **Hapus Tabel 4.16** (atau pindah ke lampiran dengan disclaimer keterbatasan alat ukur).
2. Tulis ulang **4.3.2** dan **4.4.2** bertumpu pada metrik **andal dari event log**:
   - **shuffle read/write** (hanya ETL >0 → hanya ETL yang benar-benar berkomputasi di Spark),
   - **spill memori/disk** (hanya ETL, dan tumbuh tajam 66 MB SF5 → 1.598 MB SF10 → tekanan
     memori menskala),
   - argumen **compute-bound (ETL) vs I/O-bound (fase Spark ELT/Hibrida) vs database-bound
     (transformasi dbt)** tetap bisa dipertahankan **tanpa** angka CPU%/RSS: fase Spark ELT/Hibrida
     nol shuffle/spill ⇒ murni I/O; transformasi berjalan di PostgreSQL ⇒ database-bound.
3. Di **Tabel 4.21** ganti kolom "Bukti Pendukung" dari "CPU ~340%, RSS ~2 GB" menjadi
   "shuffle 225–1.370 MB & spill hanya pada ETL".
4. Selaraskan **3.1.8.3** (metodologi) yang masih menjanjikan pengukuran "CPU dalam persen (%)
   dan memori dalam MB" — ganti menjadi metrik event log yang benar-benar dipakai (shuffle, spill,
   peak execution memory), agar metodologi = hasil.

> Catatan: kesimpulan **kualitatif** (ETL compute-bound, dst.) tidak berubah — hanya sumber
> angkanya yang perlu dipindah dari sampler ke event log. Jadi ini pekerjaan penyelarasan, bukan
> perombakan temuan.

## Catatan minor angka

- **`peak_execution_memory`** dipakai di 4.4.2 ("2,7–3,9 GB") dari Tabel 4.19. Nilai ini ambigu
  (jumlah per-task vs maks per-task) — pada rebuild xlsx, kolom peak memory adalah bagian yang
  paling tidak stabil. Aman dipakai sebagai penunjang kualitatif, tapi jangan dijadikan klaim presisi.
- **SF10 shuffle/spill tidak punya tabel sendiri** di 4.3.3 (hanya Tabel 4.19 untuk SF5). Klaim
  "spill 1.598 MB pada SF10" hanya muncul di narasi/BAB 5. Pertimbangkan menambah tabel I/O
  Spark SF10 agar klaim spill SF10 punya rujukan tabel.
