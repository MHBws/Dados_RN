import os
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from glob import glob
from pathlib import Path
from datetime import datetime
from typing import Dict, List, TypedDict, Union, Optional

import pandas
from numpy import float64
from pandas import Series

processed_files = 0
written_outputs = 0

write_lock = threading.Lock()
progress_lock = threading.Lock()

STATE_DICT = {
    "AC": "Acre",
    "AP": "Amapá",
    "AM": "Amazonas",
    "PA": "Pará",
    "RO": "Rondônia",
    "RR": "Roraima",
    "TO": "Tocantins",
    "AL": "Alagoas",
    "BA": "Bahia",
    "CE": "Ceará",
    "MA": "Maranhão",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RN": "Rio Grande do Norte",
    "SE": "Sergipe",
    "DF": "Distrito Federal",
    "GO": "Goiás",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "ES": "Espírito Santo",
    "MG": "Minas Gerais",
    "RJ": "Rio de Janeiro",
    "SP": "São Paulo",
    "PR": "Paraná",
    "RS": "Rio Grande do Sul",
    "SC": "Santa Catarina"
}

MONTH_DICT = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


class YearData(TypedDict):
    """
    A TypedDict representing yearly data.

    Attributes:
        precipitation (float): The total precipitation for the year, measured in millimeters.
        temperature_avg (float): The average temperature for the year, measured in Celsius.
    """

    precipitation: float64
    temperature_avg: float64


class PreProcessedData(TypedDict):
    """
    PreProcessedData is a TypedDict that represents the structure of pre-processed data.

    Attributes:
        uf (str): The state abbreviation (e.g., 'SP' for São Paulo).
        day_and_month (Series): A pandas Series containing day and month information.
        precipitation (Series): A pandas Series containing precipitation data.
        temp_max (Series): A pandas Series containing maximum temperature data.
        temp_min (Series): A pandas Series containing minimum temperature data.
    """

    uf: str
    day_and_month: Series
    precipitation: Series
    temp_max: Series
    temp_min: Series


class OutputData(TypedDict):
    """
    OutputData is a TypedDict that represents the structure of output data.

    Attributes:
        uf (str): The abbreviation of the state (Unidade Federativa) in Brazil.
        year (int): The year associated with the data.
        day_and_month (str): The day and month in the format "DD/MM".
        data (YearData): The data associated with the specified year.
    """
    uf: str
    year: int
    day_and_month: Union[str, int]
    data: YearData


def convert_int_str_to_float(col: Union[str, int]) -> float64:
    """
    Converts an integer or a string representing a number into a float64 value.
    """
    
    if isinstance(col, int):
        if col >= 0:
            return float64(col)
        return float64(0.0)
    elif isinstance(col, str):
        col = col.strip()
        if col == "" or col == "-9999":
            return float64(0.0)
        try:
            find_idx = col.find(',')
            if find_idx == 0:
                col_tr = float64(col.replace(",", "0."))
            else:
                col_tr = float64(col.replace(',', '.'))
            if col_tr >= 0:
                return col_tr
        except (ValueError, TypeError):
            return float64(0.0)
    return float64(0.0)


def convert_temperature_str_to_float(col: Union[str, int]) -> float64:
    """
    Converts temperature data (string or int) to float64, handling negative values.
    """
    
    if isinstance(col, int):
        return float64(col)
    elif isinstance(col, str):
        col = col.strip()
        if col == "" or col == "-9999":
            return float64(0.0)
        try:
            find_idx = col.find(',')
            if find_idx == 0:
                col_tr = float64(col.replace(",", "0."))
            else:
                col_tr = float64(col.replace(',', '.'))
            return col_tr  # Allow negative temperatures
        except (ValueError, TypeError):
            return float64(0.0)
    return float64(0.0)


def convert_str_to_day_and_month(line: str) -> str:
    """
    Converts a date string to day/month format with robust error handling.
    """
    if not line or pandas.isna(line):
        return "1/1"  # Default fallback
    
    try:
        line = str(line).strip()
        
        # Handle different date formats
        if '/' in line:
            parts = line.split('/')
            if len(parts) >= 3:
                if len(parts[0]) == 4:  # YYYY/MM/DD
                    year, month, day = parts[0], parts[1], parts[2]
                else:  # DD/MM/YYYY
                    day, month, year = parts[0], parts[1], parts[2]
                return f"{int(day)}/{int(month)}"
        elif '-' in line:
            # Handle ISO format YYYY-MM-DD
            date = datetime.fromisoformat(line.split()[0])  # Remove time if present
            return f"{date.day}/{date.month}"
        else:
            # Try to parse as timestamp or other formats
            try:
                date = pandas.to_datetime(line)
                return f"{date.day}/{date.month}"
            except:
                return "1/1"
                
    except (ValueError, IndexError, TypeError) as e:
        print(f"Erro ao converter data '{line}': {e}")
        return "1/1"  # Default fallback


def get_files() -> List[str]:
    """
    Retrieves a list of all CSV files in the current directory and its subdirectories.
    """
    files = glob("./**/*.csv", recursive=True)
    return files


def get_path_year(path: str) -> int:
    """
    Extracts year from path with error handling.
    """
    try:
        # Try different path separators
        path_split = path.replace('/', '\\').split("\\")
        for part in path_split:
            if part.isdigit() and len(part) == 4:
                year = int(part)
                if 1900 <= year <= 2100:  # Reasonable year range
                    return year
        
        # Fallback: try to find year in filename
        filename = Path(path).stem
        for part in filename.split('_'):
            if part.isdigit() and len(part) == 4:
                year = int(part)
                if 1900 <= year <= 2100:
                    return year
        
        return 2000  # Default fallback
    except (ValueError, IndexError):
        return 2000


def show_progress(stage: str, current: int, length: int) -> None:
    """
    Displays a progress update in the console.
    """
    try:
        os.system("cls" if os.name == 'nt' else "clear")
        print(stage)
        print(f"{current}/{length}")
    except:
        pass  # Continue if screen clear fails


def read_csv(path: str) -> Optional[PreProcessedData]:
    """
    Reads a CSV file with robust error handling, now including temperature data.
    """
    try:
        # Try to read metadata
        try:
            file_metadata = pandas.read_csv(
                path, encoding="ansi", sep=";", nrows=8, header=None
            )
            metadata_dict = dict(zip(file_metadata[0], file_metadata[1]))
            uf = str(metadata_dict.get("UF:", "SP"))  # Default to SP if not found
        except:
            uf = "SP"  # Default fallback

        # Clean UF value
        uf = uf.strip().upper()
        if uf not in STATE_DICT:
            uf = "SP"  # Default to SP if unknown state

        # Try to read data with different encodings
        encodings = ["ansi", "utf-8", "latin1", "cp1252"]
        file_data = None
        
        for encoding in encodings:
            try:
                file_data = pandas.read_csv(
                    path,
                    encoding=encoding,
                    on_bad_lines="skip",  # Skip bad lines instead of warning
                    sep=";",
                    engine="python",
                    skiprows=8,
                    converters={
                        "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": convert_int_str_to_float,
                        "PRECIPITA  O TOTAL, HOR RIO (mm)": convert_int_str_to_float,
                        "TEMPERATURA M XIMA NA HORA ANT. (AUT) ( C)": convert_temperature_str_to_float,
                        "TEMPERATURA M NIMA NA HORA ANT. (AUT) ( C)": convert_temperature_str_to_float,
                        "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": convert_temperature_str_to_float,
                        "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": convert_temperature_str_to_float,
                        "DATA (YYYY-MM-DD)": convert_str_to_day_and_month,
                        "DATA": convert_str_to_day_and_month,
                        "Data": convert_str_to_day_and_month
                    },
                )
                break
            except Exception as e:
                continue
        
        if file_data is None:
            print(f"Erro ao ler arquivo: {path}")
            return None

        # Try different date column names
        date_columns = ["DATA (YYYY-MM-DD)", "DATA", "Data", "data"]
        date = None
        
        for col_name in date_columns:
            if col_name in file_data.columns:
                date = file_data[col_name]
                break
        
        if date is None:
            print(f"Coluna de data não encontrada em: {path}")
            return None

        # Try different precipitation column names
        precip_columns = [
            "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
            "PRECIPITA  O TOTAL, HOR RIO (mm)",
            "PRECIPITACAO TOTAL, HORARIO (mm)",
            "PRECIPITAÇÃO TOTAL (mm)",
            "PRECIPITACAO TOTAL (mm)",
            "PRECIPITAÇÃO",
            "PRECIPITACAO"
        ]
        
        precipitation_data = None
        for col_name in precip_columns:
            if col_name in file_data.columns:
                precipitation_data = file_data[col_name]
                break
        
        if precipitation_data is None:
            print(f"Coluna de precipitação não encontrada em: {path}")
            return None

        # Try different temperature column names
        temp_max_columns = [
            "TEMPERATURA M XIMA NA HORA ANT. (AUT) ( C)",
            "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)",
            "TEMPERATURA MAXIMA NA HORA ANT. (AUT) (C)",
            "TEMPERATURA MAX NA HORA ANT. (AUT)",
            "TEMP MAX"
        ]
        
        temp_min_columns = [
            "TEMPERATURA M NIMA NA HORA ANT. (AUT) ( C)",
            "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)",
            "TEMPERATURA MINIMA NA HORA ANT. (AUT) (C)",
            "TEMPERATURA MIN NA HORA ANT. (AUT)",
            "TEMP MIN"
        ]
        
        temp_max_data = None
        temp_min_data = None
        
        for col_name in temp_max_columns:
            if col_name in file_data.columns:
                temp_max_data = file_data[col_name]
                break
        
        for col_name in temp_min_columns:
            if col_name in file_data.columns:
                temp_min_data = file_data[col_name]
                break
        
        if temp_max_data is None or temp_min_data is None:
            print(f"Colunas de temperatura não encontradas em: {path}")
            # Create empty series to maintain structure
            temp_max_data = pandas.Series(dtype=float64)
            temp_min_data = pandas.Series(dtype=float64)

        # Clean and validate data
        if date is not None:
            date = date.dropna()
        if precipitation_data is not None:
            precipitation_data = precipitation_data.dropna()
        if temp_max_data is not None:
            temp_max_data = temp_max_data.dropna()
        if temp_min_data is not None:
            temp_min_data = temp_min_data.dropna()

        data: PreProcessedData = {
            "uf": uf,
            "day_and_month": date,
            "precipitation": precipitation_data,
            "temp_max": temp_max_data,
            "temp_min": temp_min_data,
        }

        return data
    
    except Exception as e:
        print(f"Erro ao processar arquivo {path}: {e}")
        return None


def process_file(file_path: str, pre_processed_data: Dict[int, Dict[str, List[PreProcessedData]]], total_files: int) -> None:
    """
    Processa um único arquivo CSV com tratamento de erros robusto.
    """
    global processed_files

    try:
        year = get_path_year(file_path)
        data = read_csv(file_path)
        
        if data is None:
            processed_files += 1
            return

        with progress_lock:
            if year not in pre_processed_data:
                pre_processed_data[year] = {}

            if data["uf"] not in pre_processed_data[year]:
                pre_processed_data[year][data["uf"]] = []
            
            pre_processed_data[year][data["uf"]].append(data)
        
        processed_files += 1
        show_progress("Lendo arquivos...", processed_files, total_files)
        
    except Exception as e:
        print(f"Erro ao processar arquivo {file_path}: {e}")
        processed_files += 1


def process_state_data(year: int, state_data: Dict[str, List[PreProcessedData]]) -> List[OutputData]:
    """
    Processes data for a specific year and state with error handling, now including temperature.
    """
    output_data: List[OutputData] = []

    try:
        for state, pre_data in state_data.items():
            if not pre_data:
                continue
                
            # Filter out None values and empty data
            valid_data = [data for data in pre_data if data is not None and 
                         data.get("day_and_month") is not None and 
                         data.get("precipitation") is not None]
            
            if not valid_data:
                continue

            try:
                # Combine data more safely
                dataframes = []
                for data in valid_data:
                    try:
                        # Calculate average temperature from max and min
                        temp_avg_series = pandas.Series(dtype=float64)
                        
                        if (len(data["temp_max"]) > 0 and len(data["temp_min"]) > 0 and
                            len(data["temp_max"]) == len(data["temp_min"])):
                            temp_avg_series = (data["temp_max"] + data["temp_min"]) / 2
                        
                        df = pandas.DataFrame({
                            "day_and_month": data["day_and_month"], 
                            "precipitation": data["precipitation"],
                            "temp_avg": temp_avg_series if len(temp_avg_series) > 0 else pandas.Series([0.0] * len(data["day_and_month"]), dtype=float64)
                        })
                        dataframes.append(df)
                    except Exception as e:
                        print(f"Erro ao criar DataFrame para {state}: {e}")
                        continue
                
                if not dataframes:
                    continue
                
                combined_data = pandas.concat(dataframes, ignore_index=True)
                
                # Extract month more safely
                def extract_month(x):
                    try:
                        if pandas.isna(x) or x is None:
                            return 13
                        parts = str(x).split("/")
                        if len(parts) >= 2:
                            month = int(parts[1])
                            return month if 1 <= month <= 12 else 13
                        return 13
                    except:
                        return 13

                combined_data["month"] = combined_data["day_and_month"].apply(extract_month)
                
                # Filter out invalid months
                combined_data = combined_data[combined_data["month"] != 13]
                
                if combined_data.empty:
                    continue

                # Group by month and calculate aggregations
                grouped_data = combined_data.groupby("month", as_index=False).agg({
                    "precipitation": "sum",
                    "temp_avg": "mean"
                })

                for _, row in grouped_data.iterrows():
                    if pandas.notna(row["month"]) and pandas.notna(row["precipitation"]):
                        temp_avg = row["temp_avg"] if pandas.notna(row["temp_avg"]) else 0.0
                        
                        output_data.append({
                            "uf": STATE_DICT.get(state, state),
                            "year": year,
                            "day_and_month": MONTH_DICT.get(int(row["month"]), "Desconhecido"),
                            "data": {
                                "precipitation": round(float(row["precipitation"]), 2),
                                "temperature_avg": round(float(temp_avg), 2)
                            }
                        })
                        
            except Exception as e:
                print(f"Erro ao processar dados do estado {state} no ano {year}: {e}")
                continue

    except Exception as e:
        print(f"Erro ao processar dados do ano {year}: {e}")

    return output_data


def write_output_to_file(output: OutputData) -> None:
    """
    Writes output data to file with error handling, now including temperature.
    """
    try:
        path = Path("./output.csv")
        exists = path.exists()

        data = {
            "Ano": output["year"],
            "Mês": output["day_and_month"],
            "UF": output["uf"],
            "Precipitação Total": f"{output['data']['precipitation']}",
            "Temperatura Média": f"{output['data']['temperature_avg']}",
        }

        if exists:
            try:
                open_csv = pandas.read_csv("./output.csv", sep=";", encoding="ansi")
                dataframe = pandas.DataFrame([data])
                combined_dataframe = pandas.concat([open_csv, dataframe], ignore_index=True)
            except:
                combined_dataframe = pandas.DataFrame([data])
        else:
            combined_dataframe = pandas.DataFrame([data])

        combined_dataframe.to_csv(
            path_or_buf="./output.csv", index=False, mode="w", encoding="ansi", sep=";"
        )
    except Exception as e:
        print(f"Erro ao escrever arquivo: {e}")


def write_output_thread_safe(output: OutputData, total_outputs: int) -> None:
    """
    Escreve os dados de saída no arquivo de forma thread-safe.
    """
    global written_outputs

    try:
        with write_lock:
            write_output_to_file(output)
    except Exception as e:
        print(f"Erro ao escrever dados: {e}")
    
    with progress_lock:
        written_outputs += 1
        show_progress("Escrevendo resultados...", written_outputs, total_outputs)


def main():
    """
    Main function with improved error handling.
    """
    global processed_files, written_outputs
    
    try:
        file_paths = get_files()
        files_len = len(file_paths)

        if files_len == 0:
            print("Nenhum arquivo CSV encontrado!")
            return

        file_paths.sort()
        pre_processed_data: Dict[int, Dict[str, List[PreProcessedData]]] = {}

        print(f"Processando {files_len} arquivos...")

        # Read files in parallel with reduced workers to avoid overwhelming
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for file_path in file_paths:
                future = executor.submit(process_file, file_path, pre_processed_data, files_len)
                futures.append(future)
            
            # Wait for all futures to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Erro em thread de leitura: {e}")

        print("Processando dados...")
        
        # Process data in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for year, state_data in pre_processed_data.items():
                future = executor.submit(process_state_data, year, state_data)
                futures.append(future)
            
            results = []
            for future in futures:
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Erro em thread de processamento: {e}")

        output = [item for sublist in results for item in sublist]
        output_len = len(output)

        if output_len == 0:
            print("Nenhum dado válido encontrado!")
            return

        print(f"Escrevendo {output_len} registros...")

        # Write output in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for data in output:
                future = executor.submit(write_output_thread_safe, data, output_len)
                futures.append(future)
            
            # Wait for all futures to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Erro em thread de escrita: {e}")

        print("Processamento concluído!")

    except Exception as e:
        print(f"Erro na função main: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()