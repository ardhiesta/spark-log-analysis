# Setup Cluster Google Cloud Platform (GCP) untuk Pengolahan Big Data 

## Asumsi
	
Sudah mengaktifkan account GCP (trial atau non-trial)


* Langkah-langkah yang tertulis di sini menggunakan Linux Ubuntu

## Langkah-langkah

### Mempersiapkan Storage dan Cluster

1. Membuat bucket storage di GCP

	a. Buka Google Cloud console (https://console.cloud.google.com/) --> pilih Project

	b. Klik Storage di side menu, klik tombol Create bucket

	c. Isi Name

	d. Pilih Default storage class dan Location, beda konfigurasi : beda harga

	Konfigurasi yang saya pilih

	Default storage class : **Nearline**

	Location : **asia-southeast1**

	e. Klik Create, selesai
	
2. Membuat cluster Dataproc di GCP

	a. Di console GCP pilih Dataproc dari side menu

	b. Klik Enable API (jika belum di-enable)

	c. Klik tombol Create cluster

	Isi Name cluster

	Region : **asia-southeast1**

	Zone : **asia-southeast1-a**

	Cluster mode : **Standard**

	Machine type : **1 vCPU**

	Primary disk size : **32 GB**

	Nodes : **5**

	Local SSDs : **0**

	d. Klik Preemptible workers, bucket, network, version, initialization, & access options 

	Cloud storage staging bucket

	  Ketik nama bucket yang sudah dibuat di langkah 1c

	Klik tombol Initialization actions 

	  Ketik **gs://dataproc-initialization-actions/jupyter/jupyter.sh** (untuk menginstall Jupyter notebook)

	e. Klik tombol Create

### Mempersiapkan dataset

1. Dataset berupa file access log Apache, harus diupload ke Storage bucket GCP

2. Untuk menghemat biaya, Storage bucket beserta cluster GCP selalu saya hapus setelah selesai digunakan. Tentu akan melelahkan apabila harus upload berulang kali ke Storage bucket

**[TIPS]**

a. Upload dataset (file access log) ke Google Drive, sebaiknya dikompres dulu agar ukurannya lebih kecil

b. Download dataset dari Google Drive ke master node GCP

Caranya dengan membuka Terminal master node

Pilih Dataproc dari side menu di cloud console

Pilih VM instances kemudian klik tombol SSH di master node (master node biasanya diberi nama : nama_cluster-m)

Download gdrive (command line tool untuk mendownload file dari Google Drive), eksekusi perintah berikut dari Terminal SSH hasil klik tombol SSH pada langkah sebelumnya

`wget -O gdrive https://docs.google.com/uc?id=0B3X9GlR6EmbnQ0FtZmJJUXEyRTA&export=download`

Beri akses execute

`chmod +x gdrive`

Download dataset dari Google Drive
  
`./gdrive downlod [file ID]`

3. Menghubungkan master node ke Storage bucket

Install gcsfuse (tool untuk memount Storage bucket GCP di master node)

    export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s` &&

    echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list &&

    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add - &&

    sudo apt update &&

    sudo apt install gcsfuse

Buat folder baru untuk mounting Storage bucket

    mkdir storage &&

    gcsfuse ardhiestabucket storage

Ekstrak file dataset / access log (apabila masih terkompres)

Move / copy file dataset ke folder Storage bucket

    mv -v nama_file storage/

### Membuka Jupyter notebook yang terinstall cluster melalui web browser

1. Install Cloud SDK (tool untuk memanage cluster GCP melalui terminal) di laptop
	
  https://cloud.google.com/sdk/

2. Inisialisasi Cloud SDK

	`gcloud init`

3. Eksekusi perintah di terminal untuk membuat koneksi ke cluster

	`gcloud compute ssh --zone=asia-southeast1-a --ssh-flag="-D" --ssh-flag="10000" --ssh-flag="-N" "nama_cluster-m"`

4. Eksekusi perintah untuk membuka cluster melalui web browser

	`google-chrome "http://nama_cluster-m:8123" --proxy-server="socks5://localhost:10000" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/gcp`