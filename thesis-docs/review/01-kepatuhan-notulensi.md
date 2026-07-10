# 01 — Kepatuhan ke Notulensi Bimbingan

Basis: instruksi Pak Faizal Johan Atletiko dari notulensi pra-TA dan TA (bimb 1–3).
Status: ✅ sudah dipenuhi · ⚠️ sebagian/berisiko · ❌ belum dipenuhi.

## ❌ 1. Framing "Big Data" masih dominan

**Instruksi:** *"Jangan bingkai penelitian dengan Big Data — ranahnya tidak sampai sebesar itu."*

**Kondisi di PDF:** framing Big Data + 5V masih hadir di banyak tempat inti:
- **Abstrak** — kalimat pembuka soal "pertumbuhan volume dan varietas data" (masih aman, umum),
  tetapi Latar Belakang lanjutannya masuk 5V penuh.
- **Latar Belakang (hal. 9–11):** "karakteristik big data yang dikenal sebagai '5V': Volume,
  Variety, Velocity, Value, Veracity", dan penutup: *"analisis komparatif … kemampuan adaptasi
  terhadap karakteristik 5V big data (Volume, Velocity, Variety, Value, Veracity)"*.
- **Subbab 2.2.1 berjudul "Karakteristik dan Tantangan pada Big Data"** — satu subbab penuh 5V.
- Kata "big data" muncul belasan kali di BAB 1–2.

**Rekomendasi:**
- Judul/isi **2.2.1** diarahkan ulang ke *"Tantangan Integrasi Data Modern"* atau digabung ke 2.2.4
  (Integrasi Data). Pertahankan penjelasan Volume/Variety **seperlunya** sebagai konteks integrasi
  data, bukan sebagai kerangka utama.
- Di Latar Belakang, ganti klaim "adaptasi terhadap 5V" menjadi klaim yang benar-benar diukur:
  **waktu pemrosesan, efisiensi sumber daya, penyimpanan, dan skalabilitas terhadap volume**.
- Penyebutan di judul referensi (jurnal *Big Data and Cognitive Computing*, IEEE Big Data) **tidak
  perlu diubah** — itu nama sumber.

**Catatan penting soal over-claim:** Latar Belakang menyebut indikator *"tata kelola"* dan 5V
penuh (termasuk **Value** dan **Veracity**) sebagai yang dianalisis. Penelitian ini **tidak mengukur**
tata kelola, Value, maupun Veracity. Ini over-claim scope yang mudah dijadikan bahan pertanyaan
penguji. Samakan indikator yang dijanjikan di BAB 1 dengan yang benar-benar diukur di BAB 4
(waktu, CPU/memori, penyimpanan, skalabilitas).

## ❌ 2. "Kuantitatif dan kualitatif" pada Rumusan Masalah

**Instruksi:** metrik harus **kuantitatif, bukan kualitatif**.

**Kondisi:** Subbab 1.2 (hal. 11) membuka dengan *"analisis komparatif secara kuantitatif dan
kualitatif terhadap arsitektur data modern"*. Ini juga **tidak konsisten** dengan Subbab 3.1.1 yang
sudah menulis *"secara kuantitatif"* saja, dan dengan seluruh metrik BAB 4 yang murni kuantitatif.

**Rekomendasi:** hapus "dan kualitatif" → *"analisis komparatif secara kuantitatif"*. Seluruh rumusan
masalah 1–4 memang pertanyaan kuantitatif.

## ⚠️ 3. "Cleaning / pembersihan data" — hati-hati, TPC-H tidak perlu dibersihkan

**Instruksi:** cleaning **tidak jadi** dilakukan (tidak ada referensi bahwa TPC-H perlu cleaning);
referensi cleaning hanya diperlukan bila memang melakukan cleaning.

**Kondisi:** istilah "pembersihan data" dipakai di dua konteks yang perlu dibedakan:
- **Fase "Clean" Hibrida** — di kode nyata hanya *column pruning + cast tipe* (memilih 2–6 kolom
  Query 9 lalu `.cast()`), **bukan** cleaning dalam arti hapus duplikat/null/noise. BAB 4.2.3 sebagian
  besar sudah menyebutnya "pembersihan ringan … pemilihan kolom dan casting" (baik), **tetapi**
  Subbab 2.2.7 (teori ECLT) mendeskripsikan langkah Clean sebagai *"pembersihan data, penghapusan
  duplikasi, penghapusan noise"* — deskripsi teori ini bisa disalahpahami sebagai yang dikerjakan.
- **Fase Transformasi ETL** — Subbab 3.1.5 menulis: *"Tahap transformasi mencakup pembersihan
  data (penanganan data duplikat dan null)…"*. **Kode ETL tidak melakukan dedup/null-handling**
  sama sekali (hanya join/filter/agregasi Query 9). Ini klaim yang tidak sesuai implementasi.

**Rekomendasi:**
- Di **3.1.5**, buang "pembersihan data (penanganan data duplikat dan null)". Ganti dengan deskripsi
  yang benar: transformasi = *penyesuaian tipe data, penggabungan (join), filter, dan agregasi*.
- Di **4.2.3**, tegaskan sekali bahwa "pembersihan" pada Hibrida = **pemangkasan kolom + casting**,
  dan **bukan** data cleaning; TPC-H sudah bersih dari sumbernya. Ini sekaligus menutup pertanyaan
  penguji "mana referensi bahwa datanya perlu dibersihkan?".

## ✅ 4. Satuan waktu = detik

**Instruksi (bimb 3):** seragamkan seluruh satuan Spark Web UI ke **detik**.
**Kondisi:** ✅ seluruh waktu di BAB 4 dalam detik. Bagus.

## ✅ 5. Rename "Pengumpulan data" → "Akuisisi data" — hampir, ada 1 sisa

**Kondisi:** subbab metodologi sudah bernama **3.1.4 Akuisisi Data** ✅. **Tetapi** baris tabel timeline
(Tabel 3.4, No. 3) masih tertulis **"Pengumpulan Data"**. Samakan menjadi "Akuisisi Data".

## ✅ 6. Migrasi pandas → PySpark

**Kondisi:** ✅ tidak ada lagi "pandas" di draf; seluruh ekstraksi/transformasi memakai PySpark.
**Sisa:** istilah **"chunking"** (5×) adalah peninggalan rencana era pandas — lihat [03](03-verifikasi-kode.md) §"chunking".

## ✅ 7. Spark Web UI dapat diekspor/disimpan

**Instruksi (bimb 2):** pastikan monitoring Spark Web UI bisa diekspor/disimpan.
**Kondisi:** ✅ skrip mengaktifkan `spark.eventLog.enabled=true` dan menyimpan ke `logs/spark-logs`,
dan BAB 4 memang memakai event log sebagai sumber metrik andal. Bagus dan patut ditonjolkan.

## ✅ 8. Tambahkan TA mas Ihsan ke Penelitian Terdahulu

**Kondisi:** ✅ ada di Tabel 2.6 (Ihsan Kamil Al Ghozi, 2025) dan di DAFTAR PUSTAKA.

## ⚠️ 9. BAB 5 = bandingkan ke teori + penelitian terdahulu, jangan over-analisis

**Instruksi (bimb 3):** BAB 5 membandingkan hasil dengan teori dan penelitian terdahulu; jangan
over-analisis; BAB 4 diakhiri analisa hasil.

**Kondisi:** BAB 4 sudah berakhir dengan analisa (4.4) ✅. BAB 5 ("Kesimpulan dan Saran") ringkas
dan menjawab 4 rumusan masalah ✅. **Namun** BAB 5 nyaris tidak menautkan hasil ke penelitian
terdahulu (Farhan/ECLT, Haryono, Sivabalan) maupun teori. Kaitan itu memang **ada** di BAB 4.4.1
(paragraf "berbeda dengan sebagian penelitian terdahulu yang menyatakan transformasi paling
membebani…"), tapi bila pembimbing mengharapkan sintesis "hasil vs teori/prior work" secara
eksplisit, pertimbangkan menambah **1 paragraf** di BAB 5 yang menyandingkan:
- Temuan "ELT paling lambat di on-premise single-node" ↔ klaim prior work (Haryono/Sivabalan)
  bahwa ELT unggul — jelaskan bahwa keunggulan itu bersyarat lingkungan cloud/MPP (sudah
  disinggung di 4.4.1, tinggal diangkat ke kesimpulan).
- Temuan "bottleneck di pemuatan, bukan transformasi" ↔ Rongala/Farhan yang menempatkan
  transformasi sebagai bottleneck.

## ⚠️ 10. Kode di laporan = outline/big picture, bukan tutorial (bimb 1)

**Kondisi:** BAB 4.2 menampilkan **38 listing kode** (Kode 4.1–4.38), sebagian sangat granular
(mis. tiap model staging `stg_*` ditampilkan utuh satu per satu; konfigurasi koneksi `.env`; dua
perintah `dbt run` terpisah). Ini condong ke "tutorial", berlawanan dengan arahan "outline saja jika
kompleks". Pertimbangkan:
- Ringkas 6 model staging ELT menjadi **satu** contoh + kalimat "model tabel lain serupa
  (lihat Lampiran)".
- Pindahkan listing penuh ke **Lampiran**, sisakan di badan tulisan potongan yang benar-benar
  menjelaskan perbedaan antar-arsitektur (ekstraksi multi-format, fase clean Hibrida, satu mart).
- Detail lengkap juga membuat inkonsistensi kode (lihat [03](03-verifikasi-kode.md)) makin banyak
  titik yang harus dijaga akurat.

## ✅ 11. Timeline horizontal, 1 bulan = 4 minggu

**Kondisi:** ✅ Tabel 3.4 sudah horizontal, 5 bulan × 4 minggu. (Perbaiki hanya label baris
"Pengumpulan Data" → "Akuisisi Data", lihat §5.)
