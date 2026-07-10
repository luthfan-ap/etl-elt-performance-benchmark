# 05 — Bahasa, Storytelling, & Standar Penulisan TA ITS

Secara umum register tulisan **sudah tepat** untuk skripsi Indonesia: deklaratif, sebab-akibat,
tidak berlebihan. BAB 4.4 dan BAB 5 adalah bagian terkuat. Catatan di bawah bersifat perbaikan
halus, bukan perombakan.

## A. Kesalahan tik/tata bahasa yang perlu diperbaiki

- **Abstrak, kalimat 2:** *"…di berbagai sektor. sehingga menuntut…"* — tanda titik salah, huruf
  kecil. Ganti koma: *"…di berbagai sektor, sehingga menuntut…"*.
- **Abstrak:** kalimat *"Penelitian ini bertujuan untuk mengevaluasi…"* muncul **dua kali** hampir
  identik (kalimat ke-6 dan ke-7). Gabungkan agar tidak redundan.
- **4.2.2** (hal. 52): *"membentuk tabel akhir **dngan**"* → "dengan".
- **4.2.3.2** (hal. 65): *"berguna **sebagia** penghubung"* → "sebagai".
- **Tabel 4.18:** "partsuppp" → "partsupp" (juga di [02](02-placeholder-dan-stale.md)).
- Penulisan **desimal & ribuan** belum konsisten (koma desimal vs titik ribuan). Contoh "1,954 MB"
  yang seharusnya "1.954 MB". Lakukan satu pass penyeragaman (Indonesia: koma = desimal, titik = ribuan).
- **Kapital tak konsisten:** "Data Warehouse" vs "data warehouse", "Hibrida" vs "hibrida" muncul
  bergantian. Pilih satu konvensi (disarankan huruf kecil untuk istilah umum kecuali di judul).
- **"JSON" vs "JSONL":** batasan masalah & abstrak menyebut format "JSON", sedangkan implementasi
  konsisten "JSONL". Beri satu kalimat penjelas sekali di awal ("JSON yang dipakai adalah JSONL /
  newline-delimited JSON") lalu pakai "JSONL" seterusnya, agar tidak terbaca sebagai dua hal berbeda.

## B. Kalimat "kurang skripsi" (agak nge-blog) — sarankan diperhalus

Style yang dihindari: penekanan dramatis, "tidak sekadar…, melainkan…", "yang lebih menarik",
"temuan yang paling menarik". Beberapa muncul dan sebaiknya dinetralkan.

### B1. 4.3.1.1 (hal. 73)
**Sekarang:**
> "…selisih antara skenario terbaik dan terburuk mencapai sekitar 35 kali lipat. Rentang yang
> selebar ini menunjukkan bahwa pemilihan arsitektur dan format data bukanlah keputusan yang
> berdampak kecil, melainkan faktor yang secara fundamental menentukan efisiensi waktu…"

**Usulan (lebih datar, tetap berisi):**
> "…selisih antara skenario tercepat dan terlambat mencapai sekitar 35 kali lipat. Rentang sebesar
> ini menunjukkan bahwa pemilihan arsitektur dan format data berpengaruh besar terhadap efisiensi
> waktu integrasi data pada lingkungan pengujian."

### B2. 4.4.1 intro (hal. 79)
**Sekarang:**
> "…ETL tidak sekadar lebih cepat, melainkan lebih cepat secara berlipat ganda: pada format CSV,
> ETL … sekitar 12 kali lebih cepat…"

**Usulan:**
> "…ETL unggul dengan selisih yang besar. Pada format CSV, ETL (32,49 detik) sekitar 12 kali lebih
> cepat dibanding ELT (408,26 detik), dan pada Parquet selisihnya melebar hingga sekitar 30 kali
> lipat (12,81 detik berbanding 382,33 detik)."

### B3. 4.3.1.2 (hal. 74)
> "Temuan ini bertentangan dengan asumsi umum bahwa transformasi merupakan tahap termahal…"
— ini bagus dan relevan (mengaitkan ke prior work). Pertahankan, cukup ganti "Perbedaan yang
**mencolok** ini **menarik** karena…" (hal. 74) menjadi "Perbedaan ini terjadi karena…".

### B4. 4.4.4.2 (hal. 87)
> "Temuan yang **paling menarik** dari analisis skalabilitas adalah…" → "Temuan penting dari
> analisis skalabilitas adalah…".

> Prinsip: pertahankan **kedalaman** analisis (mekanisme, sebab, kaitan teori) — cukup ganti
> pembungkus retorisnya dengan konektor deklaratif ("Hal ini disebabkan…", "Dengan demikian…",
> "Sebaliknya…").

## C. Storytelling & alur (untuk sidang)

- **Grafik dulu, baru tabel.** Sesuai arahan bimb 1, hasil sebaiknya dibuka dengan grafik. Saat ini
  4.3 langsung ke tabel angka. Tambahkan grafik (lihat [02](02-placeholder-dan-stale.md) §C) dan
  rujuk grafik itu di kalimat pembuka tiap subbab hasil.
- **Benang merah 4.3 → 4.4 sudah baik:** 4.3 menyajikan angka, 4.4 menjelaskan sebab. Pertahankan.
- **BAB 5 belum mengaitkan ke prior work secara eksplisit** (lihat [01](01-kepatuhan-notulensi.md) §9).
  Menambah 1 paragraf sintesis "hasil vs teori/penelitian terdahulu" akan memenuhi arahan bimb 3
  dan memperkuat pertahanan sidang (mis. mengapa hasil ELT di sini berbeda dari klaim Haryono/
  Sivabalan yang menyimpulkan ELT unggul — karena lingkungan cloud/MPP vs single-node on-premise).

## D. Konsistensi istilah & klaim antar-bab

- **Indikator yang dijanjikan vs diukur.** BAB 1 menjanjikan (antara lain) "tata kelola" dan "5V".
  BAB 4 mengukur waktu, CPU/memori, penyimpanan, skalabilitas. Samakan: hapus indikator yang
  tidak diukur dari BAB 1 (lihat [01](01-kepatuhan-notulensi.md) §1).
- **Metodologi vs hasil (sumber daya).** 3.1.8.3 menjanjikan CPU (%) & memori (MB); hasil yang
  dipertahankan (BAB 5) memakai shuffle/spill dari event log. Samakan metodologi ke metrik yang
  benar-benar dipakai (lihat [04](04-verifikasi-data-angka.md) §KRITIS).
- **Judul EN vs ID** pada halaman judul sudah terisi (bukan placeholder) — ✅.

## E. Sitasi & DAFTAR PUSTAKA

- Format APA umumnya rapi dan konsisten. ✅
- **Tabel 2.2** menyebut tiga peneliti (Rambabu, Althati, **Selvaraj**), tetapi DAFTAR PUSTAKA
  hanya "Rambabu, V. P., & Althati, C. (2023)" (Selvaraj hilang). Samakan.
- Beberapa entri web tanpa tanggal akses/DOI (IBM "What is Apache Parquet?", TPC Spec Book 1993).
  Lengkapi bila pedoman prodi mewajibkan.
- Klaim angka industri di Latar Belakang ("reduksi 50–90%", "biaya turun 30–40%") bersumber
  Integrate.io (blog). Untuk TA, tandai jelas sebagai klaim praktik industri (bukan temuan akademik)
  — sudah cukup dengan sitasi, tapi pastikan tidak terbaca sebagai hasil penelitian peer-reviewed.

## F. Ringkas checklist penulisan sebelum kirim

- [ ] Hapus 2 catatan draf mentah (hal. 47 & 78).
- [ ] Regenerate DAFTAR ISI/TABEL/GAMBAR/KODE; perbaiki `Error! Bookmark not defined.`
- [ ] Ganti semua "Tabel 4.x" & nomor referensi salah ke nomor pasti.
- [ ] Perbaiki listing kode: `append` (ELT/Hibrida), `KEEP_UI_OPEN`, ekstraksi 3-cabang, hapus "chunking".
- [ ] Perbaiki Tabel 4.25 (207,10) & Tabel 4.26 (susun ulang baris ETL/ELT/Hibrida).
- [ ] Putuskan nasib Tabel 4.16 (CPU%/RSS) — buang atau justifikasi; selaraskan 3.1.8.3 & 4.4.2.
- [ ] Hapus SF50 dari BAB 3 (kecuali saran future work); samakan seluruh BAB 3 ke SF5+SF10.
- [ ] Hapus "kualitatif" dari 1.2; kurangi framing Big Data/5V di BAB 1 & 2.2.1.
- [ ] Tambahkan grafik hasil (min. total waktu, per-fase, rasio skala).
- [ ] Perbaiki typo ("dngan", "sebagia", "partsuppp", titik/koma desimal).
- [ ] Tambahkan penyimpanan & skalabilitas ke Abstrak.
