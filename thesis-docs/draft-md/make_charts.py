"""
Generator grafik Bab 4 (SF5 & SF10) untuk tugas akhir.
Output PNG ke thesis-docs/draft-md/assets/.
Jalankan: python make_charts.py
Data: rata-rata 3 run dari scenario_times.csv (SF5 dari rekap xlsx, SF10 dari run 2026-07-09/10).
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

OUT = Path(__file__).resolve().parent / "assets"
OUT.mkdir(exist_ok=True)

# Palet colorblind-safe (Okabe-Ito)
C_ETL, C_ELT, C_HYB = "#0072B2", "#D55E00", "#009E73"
ARCH_C = {"ETL": C_ETL, "ELT": C_ELT, "Hibrida": C_HYB}
FMT_C = {"CSV": "#56B4E9", "JSONL": "#E69F00", "Parquet": "#009E73"}
FORMATS = ["CSV", "JSONL", "Parquet"]
ARCHS = ["ETL", "ELT", "Hibrida"]

# ---- DATA (mean detik) ----
total = {  # [csv, jsonl, parquet]
    "SF5": {"ETL":[32.49,57.01,12.81], "ELT":[408.26,451.88,382.33], "Hibrida":[193.19,207.10,149.60]},
    "SF10":{"ETL":[59.08,111.17,23.05],"ELT":[825.46,952.22,926.87],"Hibrida":[414.40,469.87,334.89]},
}
ratio = {"ETL":[1.82,1.95,1.80], "ELT":[2.02,2.11,2.42], "Hibrida":[2.15,2.27,2.24]}
# fase SF5: spark(load/transform) + dbt
phase_sf5 = {
    "ELT":{"spark":[259.77,296.76,230.39], "dbt":[148.48,155.12,151.95]},
    "Hibrida":{"spark":[135.86,154.82,96.60], "dbt":[57.32,52.29,53.00]},
}
warehouse = {"SF5":{"ELT":7.12,"Hibrida":2.39}, "SF10":{"ELT":13.34,"Hibrida":4.79}}  # GB
src_vol = {"CSV":5327, "JSONL":28095, "Parquet":1563}  # MB (volume dibaca Spark)
pq_vs_csv = {"ETL":60.6,"ELT":6.3,"Hibrida":22.6}   # % Parquet lebih cepat dari CSV (SF5)
json_oh = {"ETL":75.5,"ELT":10.7,"Hibrida":7.2}     # % overhead JSONL vs CSV (SF5)

plt.rcParams.update({"font.size":11, "axes.edgecolor":"#888",
    "axes.grid":True, "grid.color":"#e6e6e6", "grid.linewidth":0.8, "axes.axisbelow":True,
    "figure.dpi":200, "savefig.bbox":"tight", "font.family":"DejaVu Sans"})

def num(x, dec=1):
    return f"{x:.{dec}f}".replace(".", ",")

def save(fig, name):
    fig.savefig(OUT / name); plt.close(fig); print("wrote", name)

# 1) Total waktu SF5 (grouped bar, log-y karena rentang lebar)
def chart_total(scale, fname):
    fig, ax = plt.subplots(figsize=(7.2,4.4))
    x = np.arange(len(FORMATS)); w = 0.26
    for i,a in enumerate(ARCHS):
        vals = total[scale][a]
        b = ax.bar(x+(i-1)*w, vals, w, label=a, color=ARCH_C[a], edgecolor="white", linewidth=0.8)
        for rect,v in zip(b,vals):
            ax.text(rect.get_x()+rect.get_width()/2, v*1.03, num(v), ha="center", va="bottom", fontsize=8.5)
    ax.set_yscale("log")
    ax.set_xticks(x); ax.set_xticklabels(FORMATS)
    ax.set_ylabel("Total waktu pemrosesan (detik, skala log)")
    ax.set_title(f"Total Waktu Pemrosesan per Skenario ({scale})")
    ax.legend(title="Arsitektur", frameon=False)
    ax.set_axisbelow(True); ax.grid(axis="x", visible=False)
    save(fig, fname)

# 2) Breakdown fase SF5 (stacked: Spark + dbt) untuk ELT & Hibrida
def chart_phase():
    fig, axes = plt.subplots(1,2, figsize=(9,4.4), sharey=True)
    for ax,a in zip(axes,["ELT","Hibrida"]):
        x=np.arange(len(FORMATS))
        sp=phase_sf5[a]["spark"]; db=phase_sf5[a]["dbt"]
        ax.bar(x, sp, 0.55, label="Fase Spark (muat/proses)", color=ARCH_C[a], edgecolor="white")
        ax.bar(x, db, 0.55, bottom=sp, label="Transformasi dbt", color=ARCH_C[a], alpha=0.45, edgecolor="white")
        for i in range(len(FORMATS)):
            ax.text(i, sp[i]/2, num(sp[i]), ha="center", va="center", fontsize=8, color="white")
            ax.text(i, sp[i]+db[i]/2, num(db[i]), ha="center", va="center", fontsize=8)
            ax.text(i, sp[i]+db[i]+8, num(sp[i]+db[i]), ha="center", va="bottom", fontsize=8.5, fontweight="bold")
        ax.set_xticks(x); ax.set_xticklabels(FORMATS); ax.set_title(a)
        ax.legend(frameon=False, fontsize=8.5); ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("Waktu (detik)")
    fig.suptitle("Rincian Waktu per Fase: Spark vs dbt (SF5)")
    save(fig, "4.3.1-phase-breakdown.png")

# 3) Rasio skalabilitas SF10/SF5 (grouped bar + garis referensi 2,0)
def chart_ratio():
    fig, ax = plt.subplots(figsize=(7.2,4.4))
    x=np.arange(len(FORMATS)); w=0.26
    for i,a in enumerate(ARCHS):
        b=ax.bar(x+(i-1)*w, ratio[a], w, label=a, color=ARCH_C[a], edgecolor="white", linewidth=0.8)
        for rect,v in zip(b,ratio[a]):
            ax.text(rect.get_x()+rect.get_width()/2, v+0.02, num(v,2), ha="center", va="bottom", fontsize=8.5)
    ax.axhline(2.0, color="#555", ls="--", lw=1)
    ax.text(2.4, 2.02, "linear (2,0×)", fontsize=8.5, color="#555", va="bottom", ha="right")
    ax.set_xticks(x); ax.set_xticklabels(FORMATS); ax.set_ylim(0,2.7)
    ax.set_ylabel("Rasio waktu SF10 / SF5")
    ax.set_title("Skalabilitas: Rasio Pertumbuhan Waktu (data 2×)")
    ax.legend(title="Arsitektur", frameon=False); ax.grid(axis="x", visible=False)
    save(fig, "4.4.4-scalability-ratio.png")

# 4) Waktu absolut SF5->SF10 (small multiples per format, log-y)
def chart_scale_time():
    fig, axes = plt.subplots(1,3, figsize=(10.5,4), sharey=True)
    xs=[5,10]
    for ax,fi,fmt in zip(axes,range(3),FORMATS):
        for a in ARCHS:
            ys=[total["SF5"][a][fi], total["SF10"][a][fi]]
            ax.plot(xs, ys, "-o", color=ARCH_C[a], label=a, lw=2, ms=6)
            for xv,yv in zip(xs,ys):
                ax.text(xv, yv*1.08, num(yv), ha="center", fontsize=7.5)
        ax.set_yscale("log"); ax.set_xticks(xs); ax.set_xticklabels(["SF5","SF10"])
        ax.set_title(fmt); ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("Total waktu (detik, skala log)")
    axes[2].legend(title="Arsitektur", frameon=False, fontsize=8.5)
    fig.suptitle("Skalabilitas Waktu Pemrosesan: SF5 → SF10")
    save(fig, "4.4.4-scalability-time.png")

# 5) Ukuran tabel warehouse SF5 vs SF10 (grouped bar ELT vs Hibrida)
def chart_warehouse():
    fig, ax = plt.subplots(figsize=(6.4,4.4))
    labels=["ELT","Hibrida"]; x=np.arange(2); w=0.35
    s5=[warehouse["SF5"][a] for a in labels]; s10=[warehouse["SF10"][a] for a in labels]
    b1=ax.bar(x-w/2, s5, w, label="SF5", color="#94c9e8", edgecolor="white")
    b2=ax.bar(x+w/2, s10, w, label="SF10", color="#0072B2", edgecolor="white")
    for b in (b1,b2):
        for r in b:
            ax.text(r.get_x()+r.get_width()/2, r.get_height()+0.15, num(r.get_height(),1), ha="center", fontsize=9)
    ax.set_xticks(x); ax.set_xticklabels(labels)
    ax.set_ylabel("Total ukuran tabel mentah (GB)")
    ax.set_title("Skalabilitas Penyimpanan Data Warehouse")
    ax.legend(frameon=False); ax.grid(axis="x", visible=False)
    save(fig, "4.4.4-warehouse-storage.png")

# 6) Pengaruh format (SF5): Parquet vs CSV speedup + JSONL overhead
def chart_format_effect():
    fig, axes = plt.subplots(1,2, figsize=(9.5,4.2))
    x=np.arange(len(ARCHS))
    for r in axes[0].bar(x,[pq_vs_csv[a] for a in ARCHS], 0.55, color=[ARCH_C[a] for a in ARCHS], edgecolor="white"):
        axes[0].text(r.get_x()+r.get_width()/2, r.get_height()+1, num(r.get_height()), ha="center", fontsize=9)
    axes[0].set_title("Parquet lebih cepat dari CSV (%)"); axes[0].set_ylim(0,70)
    for r in axes[1].bar(x,[json_oh[a] for a in ARCHS], 0.55, color=[ARCH_C[a] for a in ARCHS], edgecolor="white"):
        axes[1].text(r.get_x()+r.get_width()/2, r.get_height()+1, num(r.get_height()), ha="center", fontsize=9)
    axes[1].set_title("Overhead JSONL vs CSV (%)"); axes[1].set_ylim(0,85)
    for ax in axes:
        ax.set_xticks(x); ax.set_xticklabels(ARCHS); ax.grid(axis="x", visible=False)
    fig.suptitle("Pengaruh Format Data terhadap Waktu per Arsitektur (SF5)")
    save(fig, "4.4.3-format-effect.png")

# 7) Volume data sumber per format (bar, log)
def chart_src_volume():
    fig, ax = plt.subplots(figsize=(6.2,4.2))
    fmts=list(src_vol.keys()); vals=[src_vol[f] for f in fmts]
    b=ax.bar(fmts, vals, 0.55, color=[FMT_C[f] for f in fmts], edgecolor="white")
    for r,v in zip(b,vals):
        ax.text(r.get_x()+r.get_width()/2, v*1.05, f"{v:,}".replace(",",".")+" MB", ha="center", fontsize=9)
    ax.set_yscale("log"); ax.set_ylabel("Volume dibaca Spark (MB, skala log)")
    ax.set_title("Volume Data Sumber per Format (SF5)")
    ax.grid(axis="x", visible=False)
    save(fig, "4.3.3-source-volume.png")

# 8) Waktu CPU eksekutor (dtk) SF5 — grouped bar (metrik andal dari event log)
cpu_time = {"ETL":[89.1,168.9,27.4], "ELT":[162.6,246.0,150.3], "Hibrida":[91.4,166.2,59.1]}
def chart_cpu_time():
    fig, ax = plt.subplots(figsize=(7.2,4.4))
    x=np.arange(len(FORMATS)); w=0.26
    for i,a in enumerate(ARCHS):
        b=ax.bar(x+(i-1)*w, cpu_time[a], w, label=a, color=ARCH_C[a], edgecolor="white", linewidth=0.8)
        for r,v in zip(b,cpu_time[a]):
            ax.text(r.get_x()+r.get_width()/2, v+3, num(v), ha="center", va="bottom", fontsize=8.5)
    ax.set_xticks(x); ax.set_xticklabels(FORMATS)
    ax.set_ylabel("Waktu CPU eksekutor (detik)")
    ax.set_title("Waktu CPU Eksekutor Spark per Skenario (SF5)")
    ax.legend(title="Arsitektur", frameon=False); ax.grid(axis="x", visible=False)
    save(fig, "4.3.2-cpu.png")

# 9) Spill ETL SF5 vs SF10 (MB) — hanya ETL yang spill; tunjukkan lonjakan skala
etl_spill = {"SF5":[66.0,0.0,0.0], "SF10":[1597.5,499.8,345.3]}
def chart_etl_spill():
    fig, ax = plt.subplots(figsize=(6.8,4.4))
    x=np.arange(len(FORMATS)); w=0.36
    b1=ax.bar(x-w/2, etl_spill["SF5"], w, label="SF5", color="#f0b48a", edgecolor="white")
    b2=ax.bar(x+w/2, etl_spill["SF10"], w, label="SF10", color="#D55E00", edgecolor="white")
    for b in (b1,b2):
        for r in b:
            if r.get_height()>0:
                ax.text(r.get_x()+r.get_width()/2, r.get_height()+15, num(r.get_height(),0), ha="center", fontsize=8.5)
    ax.set_xticks(x); ax.set_xticklabels(FORMATS)
    ax.set_ylabel("Spill memori + disk (MB)")
    ax.set_title("Tumpahan Data (Spill) Arsitektur ETL: SF5 vs SF10")
    ax.legend(frameon=False); ax.grid(axis="x", visible=False)
    save(fig, "4.3.2-spill.png")

if __name__ == "__main__":
    chart_total("SF5", "4.3.1-total-runtime.png")
    chart_phase()
    chart_ratio()
    chart_scale_time()
    chart_warehouse()
    chart_format_effect()
    chart_src_volume()
    chart_cpu_time()
    chart_etl_spill()
    print("Semua grafik tersimpan di", OUT)
