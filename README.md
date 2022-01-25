[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-f059dc9a6f8d3a56e377f745f24479a46679e63a5d9fe6f495e02850cd0d8118.svg)](https://classroom.github.com/online_ide?assignment_repo_id=6801713&assignment_repo_type=AssignmentRepo)
# pythonfiletool_regex_json

Bu repository patika.dev 153. Yemeksepeti Python Web Development Bootcamp için ödev detaylarını paylaşmak üzere tema olarak hazırlanmıştır. 

Repository içerisinde yer alan `dataregex.json` dosyada yer alan kayıtların incelenerek aşağıdaki kriterlere uygun olarak bir veritabanına kaydedilmesi beklenmektedir. 
1. Veritabanı "dataregex.db" adında bir sqlite veritabanı olacaktır.
2. Veritabanı içerisindeki tablo her seferinde yeniden oluşturulmalı farklılık olarak aktarım zamanı tablo adının yanına eklenmelidir. örn : data_20221208
3. Tablo aşağıdaki sütunları içermelidir.
  - email (eposta adresi)
  - username (kullanıcı adı)
  - isimsoyisim (isim soyisim)
  - emailuserlk (e posta kullanıcı adını ya da bir bölümünü (en az 3 harf) içeriyor mu 1/0)
  - usernamelk (kullanıcı adı ile kullacının isim ya da soyisminin bir bölümünü içeriyor mu 1/0)
  - dogumyil (Doğum Yılı)
  - dogumay  (Doğum Ayı)
  - dogumgun (Doğum Günü)
  - ulke (opsiyonel olarak bir servise gönderdiğiniz lokasyon bilgisi doğrultusunda gelen ülke bilgisinin kaydedilmesi) ya da (adres bilgisi içerisinde yer alan şehir ismi kaydedilebilir)
  - ap (Kullanıcının Aktif ya da Pasif durumda olması 1/0 Default => 1)
4. Program OOP paradigması kullanılarak yazılmalıdır. diğer dosyalardan import edilebilecek bir sınıf olarak kaydedilmelidir.
5. oluşturulacak bir main.py dosyası çalıştırılırken;
   - `--file` parametresi ile json dosyasının erişim adresini alacaktır.
   - `--db` parametresi ile veritabanı erişim adresi belirlenecektir. 

Kriterler
1. Repository olarak paylaşıldı mı?
2. Veritabanı oluşturulmuş mu?
3. Kod çalıştırıldığında aynı formatta ki farklı veriler veritabanına yeni bir tablo olarak kaydediliyor mu?
4. Program OOP paradigmasına uygun yazılmış mı?
5. main.py dosyası parametrelerle sağlıklı çalışıyor mu? parametre girilmediğinde bilgi vererek kullanıcıyı yönlendiriyor mu?
  
