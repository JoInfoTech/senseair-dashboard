from flask import Flask, jsonify
from flask_socketio import SocketIO
import paho.mqtt.client as mqtt
import json
import os
from datetime import datetime
import threading
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'senseair-secret-key')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', logger=True, engineio_logger=True)

# Configurações do banco (Render fornece DATABASE_URL)
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Configurações MQTT
MQTT_BROKER = "680d8562f36e4485989a1d686e63d56e.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "joinfotech"
MQTT_PASSWORD = "89165604Jonas"

TOPIC_DATA = "/op/aq/senseair/data"
TOPIC_STATUS = "/op/aq/senseair/status"
TOPIC_KEEPALIVE = "/op/aq/senseair/keepalive"

# Setup do banco de dados
Base = declarative_base()

class SensorData(Base):
    __tablename__ = 'sensor_data'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now)
    counter = Column(Integer)
    temperature = Column(Float)
    humidity = Column(Float)
    eco2 = Column(Integer)
    tvoc = Column(Integer)

class StatusLog(Base):
    __tablename__ = 'status_log'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now)
    message = Column(String)

# Cria engine e sessão com configurações para multi-threading
if DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=10,
        max_overflow=20,
        echo=False
    )
    Base.metadata.create_all(engine)
    
    # Usa scoped_session para thread-safety
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    
    print("✓ Banco de dados PostgreSQL conectado")
else:
    print("⚠️ DATABASE_URL não configurada")
    engine = None
    Session = None

current_data = {
    'temperature': 0,
    'humidity': 0,
    'eco2': 0,
    'tvoc': 0,
    'last_update': None,
    'status': 'Aguardando conexão...',
    'connected': False
}

def save_sensor_data(data):
    try:
        if 'data' in data and engine and Session:
            sensor_data = data['data']
            
            # Usa scoped_session que é thread-safe
            session = Session()
            try:
                record = SensorData(
                    counter=data.get('counter', 0),
                    temperature=sensor_data.get('temperature', 0),
                    humidity=sensor_data.get('humidity', 0),
                    eco2=sensor_data.get('eco2', 0),
                    tvoc=sensor_data.get('tvoc', 0)
                )
                session.add(record)
                session.commit()
                
                current_data['temperature'] = sensor_data.get('temperature', 0)
                current_data['humidity'] = sensor_data.get('humidity', 0)
                current_data['eco2'] = sensor_data.get('eco2', 0)
                current_data['tvoc'] = sensor_data.get('tvoc', 0)
                current_data['last_update'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                
                socketio.emit('sensor_update', current_data)
                
                print(f"✓ Dados salvos: Temp={sensor_data.get('temperature')}°C, eCO2={sensor_data.get('eco2')}ppm")
            finally:
                Session.remove()  # Remove a sessão do escopo atual
                
    except Exception as e:
        print(f"✗ Erro ao salvar dados: {e}")
        import traceback
        traceback.print_exc()

def save_status(status_msg):
    try:
        if engine and Session:
            session = Session()
            try:
                log = StatusLog(message=status_msg)
                session.add(log)
                session.commit()
            finally:
                Session.remove()
        
        current_data['status'] = status_msg
        socketio.emit('status_update', {'status': status_msg, 'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        
        print(f"✓ Status: {status_msg}")
    except Exception as e:
        print(f"✗ Erro ao salvar status: {e}")
        import traceback
        traceback.print_exc()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✓ Conectado ao broker MQTT")
        current_data['connected'] = True
        client.subscribe(TOPIC_DATA)
        client.subscribe(TOPIC_STATUS)
        client.subscribe(TOPIC_KEEPALIVE)
        
        print("  → Notificando todos os clientes: MQTT conectado")
        socketio.emit('mqtt_status', {'connected': True})
    else:
        print(f"✗ Falha na conexão MQTT: {rc}")
        current_data['connected'] = False
        socketio.emit('mqtt_status', {'connected': False})

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode('utf-8')
    
    if topic == TOPIC_DATA:
        try:
            data = json.loads(payload)
            save_sensor_data(data)
        except json.JSONDecodeError:
            print(f"✗ Erro ao decodificar JSON")
    
    elif topic == TOPIC_STATUS:
        save_status(payload)
    
    elif topic == TOPIC_KEEPALIVE:
        print(f"Keep Alive: {payload}")

def on_disconnect(client, userdata, rc):
    print("✗ Desconectado do MQTT")
    current_data['connected'] = False
    socketio.emit('mqtt_status', {'connected': False})

mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
mqtt_client.tls_set()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect

def start_mqtt():
    import time
    
    # Aguarda 2 segundos para o servidor estabilizar
    time.sleep(2)
    
    try:
        print(f"→ Tentando conectar ao MQTT broker: {MQTT_BROKER}:{MQTT_PORT}")
        print(f"→ Usuário MQTT: {MQTT_USER}")
        print("→ Chamando mqtt_client.connect()...")
        
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        print("→ Connect() executado com sucesso!")
        print("→ Iniciando loop MQTT...")
        
        mqtt_client.loop_start()
        
        print("→ Loop MQTT iniciado em background ✓")
        
    except Exception as e:
        print(f"✗ Erro MQTT: {e}")
        import traceback
        traceback.print_exc()

@socketio.on('connect')
def handle_connect():
    print("✓ Cliente WebSocket conectado")
    mqtt_status = current_data.get('connected', False)
    print(f"  → Enviando status MQTT para cliente: {mqtt_status}")
    socketio.emit('mqtt_status', {'connected': mqtt_status})
    
@socketio.on('disconnect')
def handle_disconnect():
    print("✗ Cliente WebSocket desconectado")

@app.route('/')
def index():
    return '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SenseAir-001 Dashboard</title>
  <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      padding: 20px;
    }
    .container { max-width: 1600px; margin: 0 auto; }
    .header {
      background: white;
      padding: 30px;
      border-radius: 15px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.2);
      margin-bottom: 20px;
      text-align: center;
    }
    .header h1 { color: #667eea; font-size: 2.5em; margin-bottom: 10px; }
    .connection-status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 16px;
      border-radius: 20px;
      font-size: 0.9em;
      font-weight: 600;
    }
    .connection-status.online { background: #d4edda; color: #155724; }
    .connection-status.offline { background: #f8d7da; color: #721c24; }
    .pulse {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      animation: pulse 2s infinite;
    }
    .pulse.online { background: #28a745; }
    .pulse.offline { background: #dc3545; }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.5; }
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 20px;
      margin-bottom: 20px;
    }
    .metric-card {
      background: white;
      padding: 25px;
      border-radius: 15px;
      box-shadow: 0 5px 15px rgba(0,0,0,0.1);
      text-align: center;
      transition: transform 0.2s;
    }
    .metric-card:hover { transform: translateY(-5px); }
    .metric-icon { font-size: 3em; margin-bottom: 10px; }
    .metric-label { color: #666; font-size: 0.9em; margin-bottom: 5px; }
    .metric-value { font-size: 2.5em; font-weight: bold; color: #667eea; }
    .metric-unit { font-size: 0.5em; color: #999; }
    .charts {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
      gap: 20px;
      margin-bottom: 20px;
    }
    .chart-card {
      background: white;
      padding: 20px;
      border-radius: 15px;
      box-shadow: 0 5px 15px rgba(0,0,0,0.1);
    }
    .chart-card h3 { color: #667eea; margin-bottom: 15px; }
    .info-panel {
      background: white;
      padding: 20px;
      border-radius: 15px;
      box-shadow: 0 5px 15px rgba(0,0,0,0.1);
      margin-bottom: 20px;
    }
    .info-panel h3 { color: #667eea; margin-bottom: 15px; }
    .last-update {
      text-align: center;
      color: #666;
      font-size: 0.9em;
      margin-top: 10px;
    }
    .stat-row {
      display: flex;
      justify-content: space-between;
      padding: 10px 0;
      border-bottom: 1px solid #eee;
    }
    .stat-row:last-child { border-bottom: none; }
    .stat-label { color: #666; }
    .stat-value { font-weight: bold; color: #667eea; }
    .log-entry {
      padding: 8px;
      margin-bottom: 5px;
      background: #f8f9fa;
      border-radius: 5px;
      font-size: 0.85em;
      display: flex;
      gap: 10px;
    }
    .log-time {
      color: #667eea;
      font-weight: 600;
      white-space: nowrap;
    }
    @media (max-width: 768px) {
      .metrics { grid-template-columns: 1fr; }
      .charts { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>🌬️ SenseAir-001 Dashboard</h1>
      <p style="color: #666; margin-bottom: 15px;">Monitoramento de Qualidade do Ar em Tempo Real</p>
      <div id="connectionStatus" class="connection-status offline">
        <span class="pulse offline"></span>
        <span>MQTT Desconectado</span>
      </div>
      <div class="last-update">Última atualização: <span id="lastUpdate">-</span></div>
    </div>

    <div class="metrics">
      <div class="metric-card">
        <div class="metric-icon">🌡️</div>
        <div class="metric-label">Temperatura</div>
        <div class="metric-value"><span id="temperatureValue">0.0</span><span class="metric-unit">°C</span></div>
      </div>
      <div class="metric-card">
        <div class="metric-icon">💧</div>
        <div class="metric-label">Umidade</div>
        <div class="metric-value"><span id="humidityValue">0.0</span><span class="metric-unit">%</span></div>
      </div>
      <div class="metric-card">
        <div class="metric-icon">🏭</div>
        <div class="metric-label">eCO₂</div>
        <div class="metric-value"><span id="eco2Value">0</span><span class="metric-unit">ppm</span></div>
      </div>
      <div class="metric-card">
        <div class="metric-icon">💨</div>
        <div class="metric-label">TVOC</div>
        <div class="metric-value"><span id="tvocValue">0</span><span class="metric-unit">ppb</span></div>
      </div>
    </div>

    <div class="charts">
      <div class="chart-card">
        <h3>📈 eCO₂ (ppm)</h3>
        <canvas id="eco2Chart"></canvas>
      </div>
      <div class="chart-card">
        <h3>📈 TVOC (ppb)</h3>
        <canvas id="tvocChart"></canvas>
      </div>
      <div class="chart-card">
        <h3>🌡️ Temperatura (°C)</h3>
        <canvas id="tempChart"></canvas>
      </div>
      <div class="chart-card">
        <h3>💧 Umidade (%)</h3>
        <canvas id="humidityChart"></canvas>
      </div>
    </div>

    <div class="info-panel">
      <h3>📊 Estatísticas</h3>
      <div id="statistics">
        <p style="text-align: center; color: #999;">Carregando...</p>
      </div>
    </div>

    <div class="info-panel">
      <h3>📋 Log de Status</h3>
      <div id="statusLog">
        <p style="text-align: center; color: #999;">Carregando...</p>
      </div>
    </div>
  </div>

  <script>
    const socket = io();
    
    socket.on('connect', () => {
      console.log('✓ WebSocket conectado');
    });

    socket.on('mqtt_status', (data) => {
      const statusEl = document.getElementById('connectionStatus');
      if (data.connected) {
        statusEl.className = 'connection-status online';
        statusEl.innerHTML = '<span class="pulse online"></span><span>Conectado ao MQTT</span>';
      } else {
        statusEl.className = 'connection-status offline';
        statusEl.innerHTML = '<span class="pulse offline"></span><span>MQTT Desconectado</span>';
      }
    });

    socket.on('sensor_update', (data) => {
      document.getElementById('temperatureValue').textContent = parseFloat(data.temperature).toFixed(1);
      document.getElementById('humidityValue').textContent = parseFloat(data.humidity).toFixed(1);
      document.getElementById('eco2Value').textContent = data.eco2;
      document.getElementById('tvocValue').textContent = data.tvoc;
      document.getElementById('lastUpdate').textContent = data.last_update || '-';
      loadHistory();
      loadStatistics();
    });

    socket.on('status_update', (data) => {
      loadStatusLog();
    });

    const chartOptions = {
      responsive: true,
      maintainAspectRatio: true,
      scales: {
        y: { beginAtZero: false },
        x: { display: true }
      },
      plugins: {
        legend: { display: false }
      }
    };

    const eco2Chart = new Chart(document.getElementById('eco2Chart'), {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: 'eCO₂',
          data: [],
          borderColor: '#667eea',
          backgroundColor: 'rgba(102, 126, 234, 0.1)',
          tension: 0.4,
          fill: true
        }]
      },
      options: chartOptions
    });

    const tvocChart = new Chart(document.getElementById('tvocChart'), {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: 'TVOC',
          data: [],
          borderColor: '#764ba2',
          backgroundColor: 'rgba(118, 75, 162, 0.1)',
          tension: 0.4,
          fill: true
        }]
      },
      options: chartOptions
    });

    const tempChart = new Chart(document.getElementById('tempChart'), {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: 'Temperatura',
          data: [],
          borderColor: '#f093fb',
          backgroundColor: 'rgba(240, 147, 251, 0.1)',
          tension: 0.4,
          fill: true
        }]
      },
      options: chartOptions
    });

    const humidityChart = new Chart(document.getElementById('humidityChart'), {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: 'Umidade',
          data: [],
          borderColor: '#4facfe',
          backgroundColor: 'rgba(79, 172, 254, 0.1)',
          tension: 0.4,
          fill: true
        }]
      },
      options: chartOptions
    });

    async function loadCurrent() {
      try {
        const res = await fetch('/api/current');
        const data = await res.json();
        document.getElementById('temperatureValue').textContent = parseFloat(data.temperature).toFixed(1);
        document.getElementById('humidityValue').textContent = parseFloat(data.humidity).toFixed(1);
        document.getElementById('eco2Value').textContent = data.eco2;
        document.getElementById('tvocValue').textContent = data.tvoc;
        document.getElementById('lastUpdate').textContent = data.last_update || '-';
      } catch (e) {
        console.error('Erro ao carregar dados atuais:', e);
      }
    }

    async function loadHistory() {
      try {
        const res = await fetch('/api/history');
        const data = await res.json();
        
        if (data.error || !data.timestamps || data.timestamps.length === 0) {
          return;
        }
        
        const labels = data.timestamps.map(t => {
          const date = new Date(t);
          return date.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
        });
        
        eco2Chart.data.labels = labels;
        eco2Chart.data.datasets[0].data = data.eco2;
        eco2Chart.update();
        
        tvocChart.data.labels = labels;
        tvocChart.data.datasets[0].data = data.tvoc;
        tvocChart.update();
        
        tempChart.data.labels = labels;
        tempChart.data.datasets[0].data = data.temperature;
        tempChart.update();
        
        humidityChart.data.labels = labels;
        humidityChart.data.datasets[0].data = data.humidity;
        humidityChart.update();
      } catch (e) {
        console.error('Erro ao carregar histórico:', e);
      }
    }

    async function loadStatistics() {
      try {
        const res = await fetch('/api/statistics');
        const data = await res.json();
        if (data.error) {
          document.getElementById('statistics').innerHTML = '<p style="text-align: center; color: #999;">Sem dados suficientes</p>';
          return;
        }
        const statsHTML = `
          <div class="stat-row"><span class="stat-label">Total de Registros</span><span class="stat-value">${data.total_records}</span></div>
          <div class="stat-row"><span class="stat-label">Temp Média</span><span class="stat-value">${data.averages.temperature}°C</span></div>
          <div class="stat-row"><span class="stat-label">eCO2 Médio</span><span class="stat-value">${data.averages.eco2} ppm</span></div>
          <div class="stat-row"><span class="stat-label">TVOC Médio</span><span class="stat-value">${data.averages.tvoc} ppb</span></div>
          <div class="stat-row"><span class="stat-label">eCO2 Máximo</span><span class="stat-value">${data.maximums.eco2} ppm</span></div>
        `;
        document.getElementById('statistics').innerHTML = statsHTML;
      } catch (e) {
        console.error('Erro ao carregar estatísticas:', e);
      }
    }

    async function loadStatusLog() {
      try {
        const res = await fetch('/api/status_log');
        const data = await res.json();
        if (data.error || !data.log || data.log.length === 0) {
          document.getElementById('statusLog').innerHTML = '<p style="text-align: center; color: #999;">Nenhum status registrado</p>';
          return;
        }
        const logHTML = data.log.reverse().map(line => {
          const match = line.match(/\\[(.*?)\\] (.*)/);
          if (match) {
            return `<div class="log-entry"><div class="log-time">${match[1]}</div><div>${match[2]}</div></div>`;
          }
          return '';
        }).join('');
        document.getElementById('statusLog').innerHTML = logHTML || '<p style="text-align: center; color: #999;">Nenhum status registrado</p>';
      } catch (e) {
        console.error('Erro ao carregar log:', e);
      }
    }

    setTimeout(() => {
      loadCurrent();
      loadHistory();
      loadStatistics();
      loadStatusLog();
    }, 1000);
    
    setInterval(loadStatistics, 30000);
  </script>
</body>
</html>'''

@app.route('/api/current')
def get_current_data():
    return jsonify(current_data)

@app.route('/api/history')
def get_history():
    try:
        if not engine or not Session:
            return jsonify({'timestamps': [], 'temperature': [], 'humidity': [], 'eco2': [], 'tvoc': []})
        
        session = Session()
        try:
            records = session.query(SensorData).order_by(SensorData.timestamp.desc()).limit(50).all()
            
            if not records:
                return jsonify({'timestamps': [], 'temperature': [], 'humidity': [], 'eco2': [], 'tvoc': []})
            
            records.reverse()
            
            history = {
                'timestamps': [r.timestamp.strftime('%Y-%m-%d %H:%M:%S') for r in records],
                'temperature': [r.temperature for r in records],
                'humidity': [r.humidity for r in records],
                'eco2': [r.eco2 for r in records],
                'tvoc': [r.tvoc for r in records]
            }
            
            return jsonify(history)
        finally:
            Session.remove()
            
    except Exception as e:
        print(f"✗ Erro em /api/history: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/statistics')
def get_statistics():
    try:
        if not engine or not Session:
            return jsonify({'error': 'Sem dados'}), 404
        
        session = Session()
        try:
            records = session.query(SensorData).all()
            
            if not records:
                return jsonify({'error': 'Sem dados'}), 404
            
            temps = [r.temperature for r in records]
            hums = [r.humidity for r in records]
            eco2s = [r.eco2 for r in records]
            tvocs = [r.tvoc for r in records]
            
            stats = {
                'total_records': len(records),
                'first_record': records[0].timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'last_record': records[-1].timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'averages': {
                    'temperature': round(sum(temps)/len(temps), 1),
                    'humidity': round(sum(hums)/len(hums), 1),
                    'eco2': round(sum(eco2s)/len(eco2s), 0),
                    'tvoc': round(sum(tvocs)/len(tvocs), 0)
                },
                'maximums': {
                    'temperature': round(max(temps), 1),
                    'humidity': round(max(hums), 1),
                    'eco2': int(max(eco2s)),
                    'tvoc': int(max(tvocs))
                },
                'minimums': {
                    'temperature': round(min(temps), 1),
                    'humidity': round(min(hums), 1),
                    'eco2': int(min(eco2s)),
                    'tvoc': int(min(tvocs))
                }
            }
            
            return jsonify(stats)
        finally:
            Session.remove()
            
    except Exception as e:
        print(f"✗ Erro em /api/statistics: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/status_log')
def get_status_log():
    try:
        if not engine or not Session:
            return jsonify({'log': []})
        
        session = Session()
        try:
            logs = session.query(StatusLog).order_by(StatusLog.timestamp.desc()).limit(20).all()
            log_lines = [f"[{log.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {log.message}" for log in logs]
            return jsonify({'log': log_lines})
        finally:
            Session.remove()
            
    except Exception as e:
        print(f"✗ Erro em /api/status_log: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'log': []}), 500

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'mqtt_connected': current_data['connected']})

@app.route('/api/mqtt_reconnect')
def mqtt_reconnect():
    """Força atualização do status MQTT"""
    try:
        # Verifica se o client MQTT está conectado
        is_connected = mqtt_client.is_connected()
        current_data['connected'] = is_connected
        
        # Notifica todos os clientes
        socketio.emit('mqtt_status', {'connected': is_connected})
        
        return jsonify({
            'mqtt_connected': is_connected,
            'message': 'Status atualizado'
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'mqtt_connected': False
        }), 500

# Inicia thread MQTT (sempre, não apenas em __main__)
print("\n" + "="*60)
print("      SenseAir-001 - Web Dashboard")
print("="*60 + "\n")

mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
mqtt_thread.start()
print("✓ Thread MQTT iniciada")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    socketio.run(app, host='0.0.0.0', port=port)
