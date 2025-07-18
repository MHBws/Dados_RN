import pandas as pd
import glob
import os
import re
from typing import List, Dict, Any, Optional
from io import StringIO

class DengueCSVProcessor:
    def __init__(self):
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
    
    def extract_year_from_filename(self, filename: str) -> int:
        """Extrai o ano do nome do arquivo"""
        # Remove extensão
        filename_without_ext = filename.replace('.csv', '').replace('.CSV', '')
        
        # Para arquivos de mortes (ex: 2022d.csv), remove o 'd'
        if filename_without_ext.endswith('d'):
            year_str = filename_without_ext[:-1]
        else:
            year_str = filename_without_ext
        
        # Extrai o ano
        if re.match(r'^\d{4}$', year_str):
            return int(year_str)
        else:
            # Fallback: procura por 4 dígitos consecutivos
            match = re.search(r'(\d{4})', filename)
            if match:
                return int(match.group(1))
            else:
                raise ValueError(f"Não foi possível extrair o ano do arquivo: {filename}")
    
    def detect_data_type(self, filepath: str) -> str:
        """Detecta se o arquivo contém dados de casos ou mortes baseado no nome do arquivo"""
        filename = os.path.basename(filepath).lower()
        
        # Remove extensão para análise
        filename_without_ext = filename.replace('.csv', '')
        
        # Identifica por padrão do nome:
        # - Casos: apenas ano (ex: 2020.csv)
        # - Mortes: ano + 'd' (ex: 2022d.csv)
        if filename_without_ext.endswith('d'):
            return 'mortes'
        elif re.match(r'^\d{4}$', filename_without_ext):
            return 'casos'
        else:
            # Fallback: tenta detectar pelo conteúdo do arquivo
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
        
        # Verifica se está na lista de colunas ignoradas
        if column_upper in self.colunas_ignoradas:
            return True
        
        # Verifica padrões específicos
        if any(keyword in column_upper for keyword in ['TOTAL', 'IGNORADO', 'EXTERIOR']):
            return True
        
        # Verifica se começa com "00" (código de ignorado/exterior)
        if column_upper.startswith('00'):
            return True
        
        return False
    
    def clean_state_name(self, state_column: str) -> str:
        """Remove o código numérico do nome do estado e retorna a sigla UF"""
        state_column = str(state_column).strip().replace('"', '')
        
        # Verifica se deve ser ignorado
        if self.should_ignore_column(state_column):
            return None
        
        # Verifica se é uma sigla direta (2 caracteres)
        if len(state_column) == 2 and state_column.isalpha():
            return state_column.upper()
        
        # Extrai código numérico do início
        match = re.match(r'(\d{2})', state_column)
        if match:
            code = match.group(1)
            # Verifica se é código de ignorado/exterior
            if code == '00':
                return None
            return self.estados_map.get(code, code)
        
        # Verifica se contém nome do estado
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
            
            # Procura pelo cabeçalho
            if any(keyword in line for keyword in ['Mês notificação', 'Mes notificacao', 'Mês', 'Mes']):
                header_line = i
                continue
            
            # Procura pelo início dos dados
            if line_clean.startswith('"Janeiro"') or line_clean.startswith('Janeiro'):
                data_start = i
                break
        
        return header_line, data_start
    
    def process_single_csv(self, filepath: str) -> List[Dict]:
        """Processa um único arquivo CSV"""
        print(f"Processando arquivo: {filepath}")
        
        # Detecta tipo de dados e extrai ano
        data_type = self.detect_data_type(filepath)
        year = self.extract_year_from_filename(os.path.basename(filepath))
        
        print(f"Tipo de dados detectado: {data_type}")
        print(f"Ano: {year}")
        
        # Lê o arquivo
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(filepath, 'r', encoding='latin1') as f:
                lines = f.readlines()
        
        # Encontra cabeçalho e início dos dados
        header_line, data_start = self.find_data_section(lines)
        
        if data_start is None:
            print(f"Não foi possível encontrar dados em {filepath}")
            return []
        
        # Extrai o cabeçalho
        if header_line is not None:
            header = lines[header_line].strip()
        else:
            header = lines[data_start].strip()
            header = header.replace('"Janeiro"', '"Mês notificação"').replace('Janeiro', 'Mês notificação')
        
        # Coleta linhas de dados
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
            return []
        
        # Processa dados
        csv_content = header + '\n' + '\n'.join(data_lines)
        
        try:
            df = pd.read_csv(StringIO(csv_content), sep=';')
        except Exception as e:
            print(f"Erro ao ler CSV: {e}")
            return []
        
        # Identifica colunas a serem removidas
        columns_to_remove = []
        for col in df.columns:
            if self.should_ignore_column(str(col)):
                columns_to_remove.append(col)
                print(f"Coluna '{col}' será ignorada")
        
        # Remove colunas indesejadas
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove, errors='ignore')
        
        # Processa dados
        records = []
        for idx, row in df.iterrows():
            mes_original = str(row.iloc[0]).strip().replace('"', '')
            
            if mes_original not in self.meses_map:
                continue
                
            mes = self.meses_map[mes_original]
            
            for col_name in df.columns[1:]:
                estado_uf = self.clean_state_name(str(col_name))
                
                # Pula se o estado deve ser ignorado
                if estado_uf is None:
                    continue
                
                valor = self.clean_data_value(row[col_name])
                
                # Cria chave única para consolidação
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
                
                # Atualiza dados baseado no tipo
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
        
        # Ordena por ano, mês e estado
        meses_ordem = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                       'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        
        df['Mes_ordem'] = df['Mes'].map({mes: i for i, mes in enumerate(meses_ordem)})
        df = df.sort_values(['Ano', 'Mes_ordem', 'Estado'])
        df = df.drop('Mes_ordem', axis=1)
        
        # Reordena colunas
        df = df[['Ano', 'Mes', 'Estado', 'Casos', 'Mortes', 'Temperatura', 'Precipitacao']]
        
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Dados consolidados salvos em: {output_file}")
        
        # Estatísticas
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
        
        # Mapeamento de variações de nomes de meses
        month_mapping = {
            'Marco': 'Marco',
            'Março': 'Marco',  # Corrige acentuação
            'Mar o': 'Marco',  # Corrige encoding
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
        
        # Mapeamento de nomes para siglas
        state_mapping = {
            'Distrito Federal': 'DF',
            'Goiás': 'GO',
            'Goi s': 'GO',  # Corrige encoding
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

if __name__ == "__main__":
    processor = DengueCSVProcessor()
    
    # Configura diretório dos dados
    dados_dir = "./dados_casos_mortes"
    
    # Debug - mostra estrutura do primeiro arquivo
    csv_files = glob.glob(os.path.join(dados_dir, "*.csv"))
    if csv_files:
        processor.show_file_structure(csv_files[0])
    
    # Processar múltiplos arquivos
    print("\n=== Processando arquivos CSV ===")
    all_records = processor.process_multiple_csvs(dados_dir)
    
    # Adicionar dados climáticos automaticamente
    climate_file = "output.csv"
    if os.path.exists(climate_file):
        print(f"\n=== Adicionando dados climáticos de {climate_file} ===")
        processor.add_climate_data(climate_file)
    else:
        print(f"\nAviso: Arquivo {climate_file} não encontrado. Continuando sem dados climáticos.")
    
    # Salvar dados consolidados
    if processor.dados_consolidados:
        df_final = processor.save_consolidated_to_csv("dengue_consolidado.csv")
        
        # Mostrar amostra dos dados
        print("\n=== Amostra dos dados consolidados ===")
        print(df_final.head(10))
        
        # Salvar versão final
        processor.save_consolidated_to_csv("dengue_final_com_clima.csv")
    else:
        print("Nenhum dado foi processado!")