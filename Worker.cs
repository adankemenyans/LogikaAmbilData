using Dapper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Collections.Concurrent; // Untuk Thread-safe dictionary

namespace CollectDataAudio
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly string _connectionString;
        private readonly List<LineConfig> _lines;
        private readonly string _baseFolderName;
        private readonly int _checkIntervalMinutes;

        // Cache untuk menyimpan kapan terakhir kali file dicek
        // Key: Full Path File, Value: LastWriteTime dari file tersebut
        private readonly ConcurrentDictionary<string, DateTime> _fileLastProcessed = new();

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = configuration.GetConnectionString("ProductionDB");
            _lines = configuration.GetSection("MonitorSettings:Lines").Get<List<LineConfig>>();
            _baseFolderName = configuration["MonitorSettings:BaseFolder"] ?? "Data Server";
            _checkIntervalMinutes = configuration.GetValue<int>("MonitorSettings:CheckIntervalMinutes, 60");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker Started. Checking every 1 hour...");

            while (!stoppingToken.IsCancellationRequested)
            {
                if (_lines != null)
                {
                    // Jalankan proses untuk setiap Line yang terdaftar
                    var tasks = _lines.Select(line => ProcessLineAsync(line, stoppingToken));
                    await Task.WhenAll(tasks);
                }

                // Standby selama 10 detik sebelum siklus berikutnya
                await Task.Delay(TimeSpan.FromMinutes(_checkIntervalMinutes), stoppingToken);
            }
        }

        private async Task ProcessLineAsync(LineConfig line, CancellationToken token)
        {
            if (string.IsNullOrEmpty(line.Ip)) return;

            string sourcePath = $@"\\{line.Ip}\{_baseFolderName}";

            try
            {
                if (!Directory.Exists(sourcePath)) return;

                // Ambil semua file CSV di folder tersebut
                var files = Directory.GetFiles(sourcePath, "*.csv");

                foreach (var file in files)
                {
                    if (token.IsCancellationRequested) break;

                    string fileName = Path.GetFileName(file);

                    // 1. FILTER FILE: Format Bulan-Tahun (Contoh: December-2025_...)
                    // Regex ini memastikan kita hanya mengambil file yang relevan sesuai screenshot
                    if (Regex.IsMatch(fileName, @"^[A-Za-z]+-\d{4}_.*", RegexOptions.IgnoreCase))
                    {
                        // 2. CEK UPDATE: Apakah file ini berubah sejak terakhir kita cek?
                        DateTime currentLastWrite = File.GetLastWriteTime(file);

                        // Jika file sudah pernah dicek DAN waktu modifikasinya belum berubah, SKIP (Standby)
                        if (_fileLastProcessed.TryGetValue(file, out DateTime lastProcessedTime))
                        {
                            if (currentLastWrite <= lastProcessedTime)
                            {
                                continue; // Tidak ada data baru di file ini
                            }
                        }

                        // Jika ada perubahan atau file baru, proses datanya
                        await ProcessSingleFile(file, line.TableName);

                        // Update waktu terakhir diproses agar loop berikutnya tidak memproses ulang jika tidak ada perubahan
                        _fileLastProcessed.AddOrUpdate(file, currentLastWrite, (key, oldValue) => currentLastWrite);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error Line {line.Name}: {ex.Message}");
            }
        }

        private async Task ProcessSingleFile(string filePath, string tableName)
        {
            string fileName = Path.GetFileName(filePath);

            try
            {
                // Gunakan FileShare.ReadWrite agar tidak error jika mesin sedang menulis ke file tersebut saat kita membacanya
                using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var sr = new StreamReader(fs))
                {
                    string headerLine = await sr.ReadLineAsync(); // Baca Header (Skip baris 1)

                    var dataToInsert = new List<ProductionData>();

                    string lineData;
                    while ((lineData = await sr.ReadLineAsync()) != null)
                    {
                        if (string.IsNullOrWhiteSpace(lineData)) continue;

                        var parts = lineData.Split(',');

                        // Validasi kolom minimal A-G (7 kolom)
                        if (parts.Length < 7) continue;

                        // --- MAPPING DATA ---
                        // Kolom A: Tanggal
                        string dateStr = parts[0].Trim();
                        if (!DateTime.TryParseExact(dateStr, "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime recordDate))
                        {
                            continue;
                        }

                        string lineName = parts[1].Trim();      // Kolom B: Line
                        string model = parts[2].Trim();         // Kolom C: Model (SESUAI REQUEST)
                        string defect = parts[3].Trim();        // Kolom D: Defect
                        string reason = parts[4].Trim();        // Kolom E: Reason
                        string station = parts[5].Trim();       // Kolom F: Station
                        int quantity = TryParseInt(parts[6]);   // Kolom G: Quantity

                        // --- CEK LOGIC DB SEBELUM INSERT (Mencegah Duplikat) ---
                        // Kita masukkan ke list dulu, nanti dicek/insert sekaligus atau per baris
                        dataToInsert.Add(new ProductionData
                        {
                            DateTime = recordDate,
                            Line = lineName,
                            Model = model,
                            Defect = defect,
                            Reason_Defect = reason,
                            Station = station,
                            Quantity = quantity
                        });
                    }

                    // --- PROSES INSERT KE DATABASE ---
                    if (dataToInsert.Count > 0)
                    {
                        using (IDbConnection db = new SqlConnection(_connectionString))
                        {
                            foreach (var item in dataToInsert)
                            {
                                // Query untuk memastikan data ini belum ada di DB
                                // Kita cek kombinasi unik: Waktu, Line, Model, Defect, Station, Reason
                                string checkQuery = $@"
                                    SELECT COUNT(1) FROM {tableName} 
                                    WHERE DateTime = @DateTime 
                                      AND Line = @Line 
                                      AND Model = @Model 
                                      AND Defect = @Defect 
                                      AND Station = @Station
                                      AND Reason_Defect = @Reason_Defect"; // Tambahkan Quantity di WHERE jika perlu sangat spesifik

                                int exists = await db.ExecuteScalarAsync<int>(checkQuery, item);

                                // Jika data belum ada (0), baru kita Insert
                                if (exists == 0)
                                {
                                    string insertQuery = $@"
                                        INSERT INTO {tableName} 
                                        (DateTime, Line, Model, Defect, Reason_Defect, Station, Quantity)
                                        VALUES 
                                        (@DateTime, @Line, @Model, @Defect, @Reason_Defect, @Station, @Quantity)";

                                    await db.ExecuteAsync(insertQuery, item);
                                    _logger.LogInformation($"New Data Inserted: {item.Model} - {item.Defect}");
                                }
                                // Jika exists > 0, worker diam saja (standby/skip) karena data sudah ada
                            }
                        }
                    }
                }
            }
            catch (IOException ioEx)
            {
                // Handle jika file sedang dikunci total oleh proses lain
                _logger.LogWarning($"File {fileName} sedang digunakan proses lain, akan dicoba lagi nanti. Info: {ioEx.Message}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Gagal memproses {fileName}: {ex.Message}");
            }
        }

        private int TryParseInt(string input)
        {
            if (int.TryParse(input, out int result)) return result;
            return 0;
        }
    }
}