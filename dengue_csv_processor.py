import pandas as pd
import glob
import os
import re
from typing import List, Dict, Any, Optional
from io import StringIO
import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime

class DengueCSVProcessor:
    def __init__(self, mysql_config: Optional[Dict] = None):
        self.meses_map = {
            'Janeiro': 'Janeiro',
            'Fevereiro': 'Fevereiro',
            'Marco': 'Marco',
            'Abril': 'Abril',
            'Maio': 'Maio',
            'Junho': 'Junho',
            'Julho': 'Julho',
            'Agosto': 'Agosto',
            'Setembro': 'Setembro',
            'Outubro': 'Outubro',
            'Novembro': 'Novembro',
            'Dezembro': 'Dezembro'
        }
        
        # Mapeamento de códigos de estado para UF
        self.estados_map = {
            '11': 'RO', '12': 'AC', '13': 'AM', '14': 'RR', '15': 'PA', '16': 'AP', '17': 'TO',
            '21': 'MA', '22': 'PI', '23': 'CE', '24': 'RN', '25': 'PB', '26': 'PE', '27': 'AL',
            '28': 'SE', '29': 'BA', '31': 'MG', '32': 'ES', '33': 'RJ', '35': 'SP', '41': 'PR',
            '42': 'SC', '43': 'RS', '50': 'MS', '51': 'MT', '52': 'GO', '53': 'DF'
        }
        
        # Nomes completos dos estados para o mapeamento
        self.estados_nomes = {
            'RO': 'Rondonia', 'AC': 'Acre', 'AM': 'Amazonas', 'RR': 'Roraima', 
            'PA': 'Para', 'AP': 'Amapa', 'TO': 'Tocantins', 'MA': 'Maranhao', 
            'PI': 'Piaui', 'CE': 'Ceara', 'RN': 'Rio Grande do Norte', 
            'PB': 'Paraiba', 'PE': 'Pernambuco', 'AL': 'Alagoas', 'SE': 'Sergipe', 
            'BA': 'Bahia', 'MG': 'Minas Gerais', 'ES': 'Espirito Santo', 
            'RJ': 'Rio de Janeiro', 'SP': 'Sao Paulo', 'PR': 'Parana', 
            'SC': 'Santa Catarina', 'RS': 'Rio Grande do Sul', 'MS': 'Mato Grosso do Sul', 
            'MT': 'Mato Grosso', 'GO': 'Goias', 'DF': 'Distrito Federal'
        }
        
        # Lista de colunas/valores a serem ignorados
        self.colunas_ignoradas = {
            'IG', 'IGNORADO', 'IGNORADO/EXTERIOR', 'EXTERIOR', 'TOTAL',
            '00', '00 IGNORADO', '00 IGNORADO/EXTERIOR', '00 Ignorado/exterior'
        }
        
        # Dados consolidados por ano/mês/estado
        self.dados_consolidados = {}
        
        # Configuração do MySQL
        self.mysql_config = mysql_config or {
            'host': 'localhost',
            'user': 'root',
            'password': 'spfc@633',
            'database': 'dengue_db',
            'port': 3306
        }
        
        # Conexão MySQL
        self.connection = None
    
    def create_mysql_connection(self) -> bool:
        """Cria conexão com o banco MySQL"""
        try:
            self.connection = mysql.connector.connect(**self.mysql_config)
            
            if self.connection.is_connected():
                print(f"Conectado ao MySQL Server versão {self.connection.get_server_info()}")
                return True
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return False
    
    def close_mysql_connection(self):
        """Fecha conexão com o banco MySQL"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Conexão MySQL fechada.")
    
    def create_database_and_tables(self) -> bool:
        """Cria o banco de dados e tabelas necessárias"""
        try:
            if not self.connection:
                # Conecta sem especificar database para criar o banco
                temp_config = self.mysql_config.copy()
                temp_config.pop('database', None)
                temp_connection = mysql.connector.connect(**temp_config)
                cursor = temp_connection.cursor()
                
                # Cria o banco de dados se não existir
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_config['database']}")
                print(f"Banco de dados '{self.mysql_config['database']}' criado/verificado.")
                
                cursor.close()
                temp_connection.close()
                
                # Agora conecta ao banco específico
                self.create_mysql_connection()
            
            cursor = self.connection.cursor()
            
            # Cria tabela principal de dados dengue
            create_dengue_table = """
            CREATE TABLE IF NOT EXISTS dengue_dados (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ano INT NOT NULL,
                mes VARCHAR(20) NOT NULL,
                estado VARCHAR(2) NOT NULL,
                casos INT DEFAULT 0,
                mortes INT DEFAULT 0,
                temperatura DECIMAL(5,2) DEFAULT 0.00,
                precipitacao DECIMAL(8,2) DEFAULT 0.00,
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_record (ano, mes, estado),
                INDEX idx_ano (ano),
                INDEX idx_estado (estado),
                INDEX idx_ano_mes (ano, mes)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            # Cria tabela de log de processamento
            create_log_table = """
            CREATE TABLE IF NOT EXISTS processamento_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                arquivo VARCHAR(255),
                tipo_dados VARCHAR(20),
                ano INT,
                registros_processados INT,
                status VARCHAR(20),
                mensagem TEXT,
                data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            # Cria tabela de estatísticas
            create_stats_table = """
            CREATE TABLE IF NOT EXISTS estatisticas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                total_registros INT,
                anos_processados JSON,
                estados_processados JSON,
                total_casos INT,
                total_mortes INT,
                data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            cursor.execute(create_dengue_table)
            cursor.execute(create_log_table)
            cursor.execute(create_stats_table)
            
            self.connection.commit()
            print("Tabelas criadas/verificadas com sucesso.")
            cursor.close()
            return True
            
        except Error as e:
            print(f"Erro ao criar banco e tabelas: {e}")
            return False
    
    def insert_data_to_mysql(self) -> bool:
        """Insere dados consolidados no MySQL"""
        if not self.connection:
            print("Não há conexão com o MySQL.")
            return False
        
        try:
            cursor = self.connection.cursor()
            consolidated_data = self.get_consolidated_data()
            
            if not consolidated_data:
                print("Nenhum dado consolidado para inserir no MySQL.")
                return False
            
            # Query para inserir ou atualizar dados
            insert_query = """
            INSERT INTO dengue_dados (ano, mes, estado, casos, mortes, temperatura, precipitacao)
            VALUES (%(Ano)s, %(Mes)s, %(Estado)s, %(Casos)s, %(Mortes)s, %(Temperatura)s, %(Precipitacao)s)
            ON DUPLICATE KEY UPDATE
                casos = VALUES(casos),
                mortes = VALUES(mortes),
                temperatura = VALUES(temperatura),
                precipitacao = VALUES(precipitacao),
                data_atualizacao = CURRENT_TIMESTAMP
            """
            
            # Insere dados em lotes
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(consolidated_data), batch_size):
                batch = consolidated_data[i:i+batch_size]
                cursor.executemany(insert_query, batch)
                total_inserted += cursor.rowcount
                
                if (i + batch_size) % 5000 == 0:
                    print(f"Processados {i + batch_size} registros...")
            
            self.connection.commit()
            print(f"Dados inseridos no MySQL com sucesso! Total de registros afetados: {total_inserted}")
            
            # Atualiza estatísticas
            self.update_statistics()
            
            cursor.close()
            return True
            
        except Error as e:
            print(f"Erro ao inserir dados no MySQL: {e}")
            self.connection.rollback()
            return False
    
    def update_statistics(self):
        """Atualiza tabela de estatísticas"""
        try:
            cursor = self.connection.cursor()
            
            # Calcula estatísticas
            stats_query = """
            SELECT 
                COUNT(*) as total_registros,
                SUM(casos) as total_casos,
                SUM(mortes) as total_mortes
            FROM dengue_dados
            """
            cursor.execute(stats_query)
            total_registros, total_casos, total_mortes = cursor.fetchone()
            
            # Anos processados
            cursor.execute("SELECT DISTINCT ano FROM dengue_dados ORDER BY ano")
            anos_processados = [row[0] for row in cursor.fetchall()]
            
            # Estados processados
            cursor.execute("SELECT DISTINCT estado FROM dengue_dados ORDER BY estado")
            estados_processados = [row[0] for row in cursor.fetchall()]
            
            # Remove estatísticas antigas
            cursor.execute("DELETE FROM estatisticas")
            
            # Insere novas estatísticas
            insert_stats = """
            INSERT INTO estatisticas (total_registros, anos_processados, estados_processados, total_casos, total_mortes)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_stats, (
                total_registros,
                json.dumps(anos_processados),
                json.dumps(estados_processados),
                total_casos or 0,
                total_mortes or 0
            ))
            
            self.connection.commit()
            cursor.close()
            print("Estatísticas atualizadas no MySQL.")
            
        except Error as e:
            print(f"Erro ao atualizar estatísticas: {e}")
    
    def log_processing(self, arquivo: str, tipo_dados: str, ano: int, registros_processados: int, status: str, mensagem: str = ""):
        """Registra log de processamento no MySQL"""
        try:
            cursor = self.connection.cursor()
            
            log_query = """
            INSERT INTO processamento_log (arquivo, tipo_dados, ano, registros_processados, status, mensagem)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(log_query, (arquivo, tipo_dados, ano, registros_processados, status, mensagem))
            self.connection.commit()
            cursor.close()
            
        except Error as e:
            print(f"Erro ao registrar log: {e}")
    
    def get_mysql_statistics(self) -> Dict:
        """Retorna estatísticas do banco MySQL"""
        if not self.connection:
            return {}
        
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            cursor.execute("SELECT * FROM estatisticas ORDER BY data_atualizacao DESC LIMIT 1")
            stats = cursor.fetchone()
            
            if stats:
                # Converte JSON strings de volta para listas
                stats['anos_processados'] = json.loads(stats['anos_processados'])
                stats['estados_processados'] = json.loads(stats['estados_processados'])
            
            cursor.close()
            return stats or {}
            
        except Error as e:
            print(f"Erro ao buscar estatísticas: {e}")
            return {}
    
    def export_mysql_to_csv(self, output_file: str = "dengue_from_mysql.csv") -> bool:
        """Exporta dados do MySQL para CSV"""
        if not self.connection:
            print("Não há conexão com o MySQL.")
            return False
        
        try:
            query = """
            SELECT ano, mes, estado, casos, mortes, temperatura, precipitacao, 
                   data_criacao, data_atualizacao
            FROM dengue_dados
            ORDER BY ano, FIELD(mes, 'Janeiro', 'Fevereiro', 'Marco', 'Abril', 'Maio', 'Junho',
                               'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'), estado
            """
            
            df = pd.read_sql(query, self.connection)
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"Dados exportados do MySQL para: {output_file}")
            print(f"Total de registros exportados: {len(df)}")
            
            return True
            
        except Error as e:
            print(f"Erro ao exportar dados do MySQL: {e}")
            return False
    
    # Métodos originais mantidos (extract_year_from_filename, detect_data_type, etc.)
    def extract_year_from_filename(self, filename: str) -> int:
        """Extrai o ano do nome do arquivo"""
        filename_without_ext = filename.replace('.csv', '').replace('.CSV', '')
        
        if filename_without_ext.endswith('d'):
            year_str = filename_without_ext[:-1]
        else:
            year_str = filename_without_ext
        
        if re.match(r'^\d{4}$', year_str):
            return int(year_str)
        else:
            match = re.search(r'(\d{4})', filename)
            if match:
                return int(match.group(1))
            else:
                raise ValueError(f"Não foi possível extrair o ano do arquivo: {filename}")
    
    def detect_data_type(self, filepath: str) -> str:
        """Detecta se o arquivo contém dados de casos ou mortes baseado no nome do arquivo"""
        filename = os.path.basename(filepath).lower()
        filename_without_ext = filename.replace('.csv', '')
        
        if filename_without_ext.endswith('d'):
            return 'mortes'
        elif re.match(r'^\d{4}$', filename_without_ext):
            return 'casos'
        else:
            print(f"Aviso: Padrão de nome não reconhecido para {filename}, tentando detectar pelo conteúdo...")
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
            except UnicodeDecodeError:
                with open(filepath, 'r', encoding='latin1') as f:
                    content = f.read()
            
            if 'Óbito pelo agravo notificado' in content or 'Obito pelo agravo notificado' in content:
                return 'mortes'
            elif 'Casos Prováveis' in content or 'Casos Provaveis' in content:
                return 'casos'
            else:
                if 'Evolução:' in content or 'Evolucao:' in content:
                    return 'mortes'
                else:
                    return 'casos'
    
    def should_ignore_column(self, column_name: str) -> bool:
        """Verifica se uma coluna deve ser ignorada"""
        column_upper = str(column_name).upper().strip().replace('"', '')
        
        if column_upper in self.colunas_ignoradas:
            return True
        
        if any(keyword in column_upper for keyword in ['TOTAL', 'IGNORADO', 'EXTERIOR']):
            return True
        
        if column_upper.startswith('00'):
            return True
        
        return False
    
    def clean_state_name(self, state_column: str) -> str:
        """Remove o código numérico do nome do estado e retorna a sigla UF"""
        state_column = str(state_column).strip().replace('"', '')
        
        if self.should_ignore_column(state_column):
            return None
        
        if len(state_column) == 2 and state_column.isalpha():
            return state_column.upper()
        
        match = re.match(r'(\d{2})', state_column)
        if match:
            code = match.group(1)
            if code == '00':
                return None
            return self.estados_map.get(code, code)
        
        state_upper = state_column.upper()
        for uf, nome in self.estados_nomes.items():
            if nome.upper() in state_upper:
                return uf
        
        return state_column
    
    def clean_data_value(self, value: Any) -> int:
        """Limpa os valores dos dados, convertendo '-' e valores vazios para 0"""
        if pd.isna(value) or value == '-' or value == '' or str(value).strip() == '':
            return 0
        try:
            clean_value = str(value).strip().replace('"', '').replace(',', '')
            return int(clean_value)
        except (ValueError, TypeError):
            return 0
    
    def find_data_section(self, lines: List[str]) -> tuple:
        """Encontra o cabeçalho e início dos dados"""
        header_line = None
        data_start = None
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            if any(keyword in line for keyword in ['Mês notificação', 'Mes notificacao', 'Mês', 'Mes']):
                header_line = i
                continue
            
            if line_clean.startswith('"Janeiro"') or line_clean.startswith('Janeiro'):
                data_start = i
                break
        
        return header_line, data_start
    
    def process_single_csv(self, filepath: str) -> List[Dict]:
        """Processa um único arquivo CSV"""
        print(f"Processando arquivo: {filepath}")
        
        data_type = self.detect_data_type(filepath)
        year = self.extract_year_from_filename(os.path.basename(filepath))
        
        print(f"Tipo de dados detectado: {data_type}")
        print(f"Ano: {year}")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(filepath, 'r', encoding='latin1') as f:
                lines = f.readlines()
        
        header_line, data_start = self.find_data_section(lines)
        
        if data_start is None:
            print(f"Não foi possível encontrar dados em {filepath}")
            if self.connection:
                self.log_processing(os.path.basename(filepath), data_type, year, 0, "ERRO", 
                                  "Não foi possível encontrar dados")
            return []
        
        if header_line is not None:
            header = lines[header_line].strip()
        else:
            header = lines[data_start].strip()
            header = header.replace('"Janeiro"', '"Mês notificação"').replace('Janeiro', 'Mês notificação')
        
        data_lines = []
        for i in range(data_start, len(lines)):
            line = lines[i].strip()
            if not line:
                continue
            if any(keyword in line for keyword in ['Total', 'Fonte', 'Notas:']):
                break
            data_lines.append(line)
        
        if not data_lines:
            print(f"Nenhuma linha de dados encontrada em {filepath}")
            if self.connection:
                self.log_processing(os.path.basename(filepath), data_type, year, 0, "ERRO", 
                                  "Nenhuma linha de dados encontrada")
            return []
        
        csv_content = header + '\n' + '\n'.join(data_lines)
        
        try:
            df = pd.read_csv(StringIO(csv_content), sep=';')
        except Exception as e:
            print(f"Erro ao ler CSV: {e}")
            if self.connection:
                self.log_processing(os.path.basename(filepath), data_type, year, 0, "ERRO", str(e))
            return []
        
        columns_to_remove = []
        for col in df.columns:
            if self.should_ignore_column(str(col)):
                columns_to_remove.append(col)
                print(f"Coluna '{col}' será ignorada")
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove, errors='ignore')
        
        records = []
        for idx, row in df.iterrows():
            mes_original = str(row.iloc[0]).strip().replace('"', '')
            
            if mes_original not in self.meses_map:
                continue
                
            mes = self.meses_map[mes_original]
            
            for col_name in df.columns[1:]:
                estado_uf = self.clean_state_name(str(col_name))
                
                if estado_uf is None:
                    continue
                
                valor = self.clean_data_value(row[col_name])
                
                key = (year, mes, estado_uf)
                
                if key not in self.dados_consolidados:
                    self.dados_consolidados[key] = {
                        'Ano': year,
                        'Mes': mes,
                        'Estado': estado_uf,
                        'Casos': 0,
                        'Mortes': 0,
                        'Temperatura': 0.0,
                        'Precipitacao': 0.0
                    }
                
                if data_type == 'casos':
                    self.dados_consolidados[key]['Casos'] = valor
                elif data_type == 'mortes':
                    self.dados_consolidados[key]['Mortes'] = valor
                
                records.append({
                    'Ano': year,
                    'Mes': mes,
                    'Estado': estado_uf,
                    'Tipo': data_type,
                    'Valor': valor
                })
        
        print(f"Processados {len(records)} registros do tipo {data_type}")
        
        # Log no MySQL se conectado
        if self.connection:
            self.log_processing(os.path.basename(filepath), data_type, year, len(records), "SUCESSO")
        
        return records
    
    def process_multiple_csvs(self, csv_directory: str, pattern: str = "*.csv") -> List[Dict]:
        """Processa múltiplos arquivos CSV de um diretório"""
        csv_files = glob.glob(os.path.join(csv_directory, pattern))
        
        if not csv_files:
            raise ValueError(f"Nenhum arquivo CSV encontrado no diretório: {csv_directory}")
        
        print(f"Encontrados {len(csv_files)} arquivos CSV")
        
        all_records = []
        
        for csv_file in sorted(csv_files):
            try:
                records = self.process_single_csv(csv_file)
                all_records.extend(records)
                print(f"Arquivo {os.path.basename(csv_file)} processado com sucesso.")
            except Exception as e:
                print(f"Erro ao processar {csv_file}: {str(e)}")
                if self.connection:
                    year = 0
                    try:
                        year = self.extract_year_from_filename(os.path.basename(csv_file))
                    except:
                        pass
                    self.log_processing(os.path.basename(csv_file), "unknown", year, 0, "ERRO", str(e))
                continue
        
        return all_records
    
    def get_consolidated_data(self) -> List[Dict]:
        """Retorna dados consolidados para o banco de dados"""
        return list(self.dados_consolidados.values())
    
    def save_consolidated_to_csv(self, output_file: str):
        """Salva os dados consolidados em CSV"""
        consolidated_data = self.get_consolidated_data()
        
        if not consolidated_data:
            print("Nenhum dado consolidado para salvar!")
            return
        
        df = pd.DataFrame(consolidated_data)
        
        meses_ordem = ['Janeiro', 'Fevereiro', 'Marco', 'Abril', 'Maio', 'Junho',
                       'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        
        df['Mes_ordem'] = df['Mes'].map({mes: i for i, mes in enumerate(meses_ordem)})
        df = df.sort_values(['Ano', 'Mes_ordem', 'Estado'])
        df = df.drop('Mes_ordem', axis=1)
        
        df = df[['Ano', 'Mes', 'Estado', 'Casos', 'Mortes', 'Temperatura', 'Precipitacao']]
        
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Dados consolidados salvos em: {output_file}")
        
        print(f"\nEstatísticas dos dados consolidados:")
        print(f"Total de registros: {len(df)}")
        print(f"Anos: {sorted(df['Ano'].unique())}")
        print(f"Estados: {sorted(df['Estado'].unique())}")
        print(f"Total de casos: {df['Casos'].sum():,}")
        print(f"Total de mortes: {df['Mortes'].sum():,}")
        
        return df
    
    def show_file_structure(self, filepath: str):
        """Mostra a estrutura do arquivo para debug"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(filepath, 'r', encoding='latin1') as f:
                lines = f.readlines()
        
        filename = os.path.basename(filepath)
        data_type = self.detect_data_type(filepath)
        year = self.extract_year_from_filename(filename)
        
        print(f"\n=== Estrutura do arquivo {filepath} ===")
        print(f"Nome do arquivo: {filename}")
        print(f"Tipo de dados: {data_type}")
        print(f"Ano extraído: {year}")
        print("Primeiras 25 linhas:")
        for i, line in enumerate(lines[:25]):
            print(f"{i:2d}: {line.strip()}")
        if len(lines) > 25:
            print("...")
    
    def normalize_month_name(self, month_name: str) -> str:
        """Normaliza nomes de meses para o padrão usado no sistema"""
        month_name = str(month_name).strip()
        
        month_mapping = {
            'Marco': 'Marco',
            'Março': 'Marco',
            'Mar o': 'Marco',
            'Maio': 'Maio',
            'Junho': 'Junho',
            'Julho': 'Julho',
            'Agosto': 'Agosto',
            'Setembro': 'Setembro',
            'Outubro': 'Outubro',
            'Novembro': 'Novembro',
            'Dezembro': 'Dezembro',
            'Janeiro': 'Janeiro',
            'Fevereiro': 'Fevereiro'
        }
        
        return month_mapping.get(month_name, month_name)
    
    def normalize_state_name(self, state_name: str) -> str:
        """Normaliza nomes de estados para siglas UF"""
        state_name = str(state_name).strip()
        
        state_mapping = {
            'Distrito Federal': 'DF',
            'Goiás': 'GO',
            'Goi s': 'GO',
            'Goias': 'GO',
            'São Paulo': 'SP',
            'Sao Paulo': 'SP',
            'Rio de Janeiro': 'RJ',
            'Minas Gerais': 'MG',
            'Bahia': 'BA',
            'Paraná': 'PR',
            'Parana': 'PR',
            'Rio Grande do Sul': 'RS',
            'Santa Catarina': 'SC',
            'Espírito Santo': 'ES',
            'Espirito Santo': 'ES',
            'Pernambuco': 'PE',
            'Ceará': 'CE',
            'Ceara': 'CE',
            'Pará': 'PA',
            'Para': 'PA',
            'Maranhão': 'MA',
            'Maranhao': 'MA',
            'Amazonas': 'AM',
            'Mato Grosso': 'MT',
            'Mato Grosso do Sul': 'MS',
            'Rondônia': 'RO',
            'Rondonia': 'RO',
            'Acre': 'AC',
            'Amapá': 'AP',
            'Amapa': 'AP',
            'Tocantins': 'TO',
            'Piauí': 'PI',
            'Piaui': 'PI',
            'Rio Grande do Norte': 'RN',
            'Paraíba': 'PB',
            'Paraiba': 'PB',
            'Alagoas': 'AL',
            'Sergipe': 'SE',
            'Roraima': 'RR'
        }
        
        # Se já é uma sigla, retorna em maiúscula
        if len(state_name) == 2 and state_name.isalpha():
            return state_name.upper()
        
        return state_mapping.get(state_name, state_name)
    
    def add_climate_data(self, climate_csv_file: str):
        """Adiciona dados climáticos aos dados consolidados"""
        try:
            # Tenta diferentes encodings
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            climate_df = None
            
            for encoding in encodings:
                try:
                    climate_df = pd.read_csv(climate_csv_file, sep=';', encoding=encoding)
                    print(f"Arquivo {climate_csv_file} lido com encoding {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if climate_df is None:
                print(f"Erro: Não foi possível ler o arquivo {climate_csv_file}")
                return
            
            print(f"Adicionando dados climáticos de {climate_csv_file}")
            print(f"Colunas encontradas: {list(climate_df.columns)}")
            
            # Normaliza nomes das colunas
            climate_df.columns = climate_df.columns.str.strip()
            
            # Mapeia colunas possíveis
            col_mapping = {}
            for col in climate_df.columns:
                col_clean = col.lower().strip()
                if 'ano' in col_clean:
                    col_mapping['Ano'] = col
                elif 'mes' in col_clean or 'mês' in col_clean:
                    col_mapping['Mes'] = col
                elif 'uf' in col_clean or 'estado' in col_clean:
                    col_mapping['Estado'] = col
                elif 'temperatura' in col_clean or 'temp' in col_clean:
                    col_mapping['Temperatura'] = col
                elif 'precipita' in col_clean or 'chuva' in col_clean:
                    col_mapping['Precipitacao'] = col
            
            print(f"Mapeamento de colunas: {col_mapping}")
            
            # Verifica se todas as colunas necessárias foram encontradas
            required_cols = ['Ano', 'Mes', 'Estado', 'Temperatura', 'Precipitacao']
            missing_cols = [col for col in required_cols if col not in col_mapping]
            
            if missing_cols:
                print(f"Aviso: Colunas não encontradas: {missing_cols}")
                return
            
            # Processa dados climáticos
            dados_climaticos_adicionados = 0
            
            for _, row in climate_df.iterrows():
                try:
                    ano = int(row[col_mapping['Ano']])
                    mes = self.normalize_month_name(row[col_mapping['Mes']])
                    estado = self.normalize_state_name(row[col_mapping['Estado']])
                    temperatura = float(row[col_mapping['Temperatura']]) if pd.notna(row[col_mapping['Temperatura']]) else 0.0
                    precipitacao = float(row[col_mapping['Precipitacao']]) if pd.notna(row[col_mapping['Precipitacao']]) else 0.0
                    
                    key = (ano, mes, estado)
                    
                    if key in self.dados_consolidados:
                        self.dados_consolidados[key]['Temperatura'] = temperatura
                        self.dados_consolidados[key]['Precipitacao'] = precipitacao
                        dados_climaticos_adicionados += 1
                    else:
                        # Cria novo registro se não existir
                        self.dados_consolidados[key] = {
                            'Ano': ano,
                            'Mes': mes,
                            'Estado': estado,
                            'Casos': 0,
                            'Mortes': 0,
                            'Temperatura': temperatura,
                            'Precipitacao': precipitacao
                        }
                        dados_climaticos_adicionados += 1
                        
                except (ValueError, TypeError) as e:
                    print(f"Erro ao processar linha: {row}, erro: {e}")
                    continue
            
            print(f"Dados climáticos adicionados com sucesso! Total: {dados_climaticos_adicionados} registros")
            
        except Exception as e:
            print(f"Erro ao adicionar dados climáticos: {e}")
            import traceback
            traceback.print_exc()

    def execute_full_pipeline_with_mysql(self, csv_directory: str, climate_file: str = "output.csv", 
                                       csv_output: str = "dengue_consolidado.csv"):
        """Executa todo o pipeline incluindo criação e inserção no MySQL"""
        print("=== INICIANDO PIPELINE COMPLETO COM MYSQL ===")
        
        # 1. Conecta ao MySQL
        print("\n1. Conectando ao MySQL...")
        if not self.create_mysql_connection():
            print("Falha na conexão. Continuando apenas com CSV...")
            return self.execute_csv_only_pipeline(csv_directory, climate_file, csv_output)
        
        # 2. Cria banco e tabelas
        print("\n2. Criando banco de dados e tabelas...")
        if not self.create_database_and_tables():
            print("Falha na criação do banco. Continuando apenas com CSV...")
            self.close_mysql_connection()
            return self.execute_csv_only_pipeline(csv_directory, climate_file, csv_output)
        
        # 3. Processa CSVs
        print(f"\n3. Processando arquivos CSV do diretório: {csv_directory}")
        try:
            all_records = self.process_multiple_csvs(csv_directory)
            print(f"Total de registros processados: {len(all_records)}")
        except Exception as e:
            print(f"Erro no processamento de CSVs: {e}")
            self.close_mysql_connection()
            return False
        
        # 4. Adiciona dados climáticos
        if os.path.exists(climate_file):
            print(f"\n4. Adicionando dados climáticos de: {climate_file}")
            self.add_climate_data(climate_file)
        else:
            print(f"\n4. Arquivo climático não encontrado: {climate_file}. Continuando sem dados climáticos.")
        
        # 5. Salva CSV consolidado
        print(f"\n5. Salvando CSV consolidado: {csv_output}")
        df_consolidado = self.save_consolidated_to_csv(csv_output)
        
        # 6. Insere dados no MySQL
        print("\n6. Inserindo dados no MySQL...")
        if self.insert_data_to_mysql():
            print("Dados inseridos no MySQL com sucesso!")
        else:
            print("Falha na inserção no MySQL.")
        
        # 7. Exibe estatísticas do MySQL
        print("\n7. Estatísticas do MySQL:")
        mysql_stats = self.get_mysql_statistics()
        if mysql_stats:
            print(f"   Total de registros no MySQL: {mysql_stats.get('total_registros', 0)}")
            print(f"   Total de casos no MySQL: {mysql_stats.get('total_casos', 0):,}")
            print(f"   Total de mortes no MySQL: {mysql_stats.get('total_mortes', 0):,}")
            print(f"   Anos processados: {mysql_stats.get('anos_processados', [])}")
            print(f"   Estados processados: {len(mysql_stats.get('estados_processados', []))} estados")
            print(f"   Última atualização: {mysql_stats.get('data_atualizacao', 'N/A')}")
        
        # 8. Opcional: Exporta do MySQL para CSV (verificação)
        verification_file = "verificacao_mysql.csv"
        print(f"\n8. Exportando dados do MySQL para verificação: {verification_file}")
        self.export_mysql_to_csv(verification_file)
        
        # 9. Fecha conexão
        print("\n9. Fechando conexão MySQL...")
        self.close_mysql_connection()
        
        print("\n=== PIPELINE COMPLETO FINALIZADO COM SUCESSO ===")
        return True

    def execute_csv_only_pipeline(self, csv_directory: str, climate_file: str = "output.csv", 
                                 csv_output: str = "dengue_consolidado.csv"):
        """Executa pipeline apenas com CSV (fallback)"""
        print("=== EXECUTANDO PIPELINE APENAS COM CSV ===")
        
        # Processa CSVs
        print(f"\nProcessando arquivos CSV do diretório: {csv_directory}")
        all_records = self.process_multiple_csvs(csv_directory)
        
        # Adiciona dados climáticos
        if os.path.exists(climate_file):
            print(f"\nAdicionando dados climáticos de: {climate_file}")
            self.add_climate_data(climate_file)
        
        # Salva CSV consolidado
        if self.dados_consolidados:
            df_final = self.save_consolidated_to_csv(csv_output)
            print("\n=== PIPELINE CSV FINALIZADO ===")
            return True
        else:
            print("Nenhum dado foi processado!")
            return False

if __name__ == "__main__":
    # Configuração do MySQL - AJUSTE CONFORME SUA CONFIGURAÇÃO
    mysql_config = {
        'host': 'localhost',          # Servidor MySQL
        'user': 'root',               # Usuário MySQL
        'password': 'spfc@633',       # Senha MySQL (deixe vazio se não houver)
        'database': 'dengue_db',      # Nome do banco de dados
        'port': 3306                  # Porta MySQL
    }
    
    # Inicializa processador com configuração MySQL
    processor = DengueCSVProcessor(mysql_config)
    
    # Configura diretório dos dados
    dados_dir = "./dados_casos_mortes"
    
    # Executa pipeline completo
    if os.path.exists(dados_dir):
        success = processor.execute_full_pipeline_with_mysql(
            csv_directory=dados_dir,
            climate_file="output.csv",
            csv_output="dengue_consolidado_final.csv"
        )
        
        if success:
            print("\nProcessamento concluído com sucesso!")
        else:
            print("\nHouve problemas no processamento.")
    else:
        print(f"Diretório não encontrado: {dados_dir}")
        
        # Debug - mostra estrutura de arquivos se houver
        csv_files = glob.glob("*.csv")
        if csv_files:
            print(f"Arquivos CSV encontrados na raiz: {csv_files}")
            processor.show_file_structure(csv_files[0])