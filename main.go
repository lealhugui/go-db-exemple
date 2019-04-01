package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	//conecta no banco, realiza a query, e salva result no csv
	params := queryOverTarget("MEU SERV")
	l := len(params)
	saveToCSV(params)
	fmt.Printf("%d\n", l)

	//serializa o array retornado pelo banco como "json", e printa seu resultado na tela
	b, err := json.Marshal(params)
	if err != nil {
		fmt.Print(err)
	} else {
		fmt.Printf("%s\n", string(b))
	}

	//cria uma variavel do tipo Parametro, e desserializa um json em seu formato
	var p Parametro
	json.Unmarshal([]byte("{\"chave\":\"a\", \"valor\": \"b\"}"), &p)
	fmt.Printf("%s\n", p)

	fmt.Printf("exec DONE\n")
}

/*****************
DEFINIÇÃO DA CLASSE "Parametro"
*****************/

//Parametro é a classe que representa 1 registro da tabela "pdv-va"."parametro"
type Parametro struct {
	Chave string `db:"chave" json:"chave"`
	Valor string `db:"valor" json:"valor"`
}

//toCSVFormat formata uma instancia de Parametro para um formato aceitavel no csv
func (p *Parametro) toCSVFormat() []string {
	return []string{
		p.Chave,
		p.Valor,
	}
}

/*****************
DEFINIÇÃO DA CLASSE "Parametro"
*****************/

//saveToCSV salva o array de parametros em csv
func saveToCSV(p []Parametro) {

	if p == nil || len(p) == 0 {
		return
	}

	file, err := os.Create("result.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	writer.Write([]string{"chave", "valor"})

	for _, param := range p {
		err := writer.Write(param.toCSVFormat())
		if err != nil {
			panic(err)
		}
	}
}

//aquireConn cria uma conexao com o DB
func aquireConn(addr string) *sqlx.DB {
	const usr = "meu user"
	const pwd = "minha senha"
	const db = "meu db"
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		usr,
		pwd,
		addr,
		db,
	)
	conn, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		panic(err)
	}
	return conn
}

//queryOverTarget abre uma conexao, e executa uma query em um banco, retornando um array de "Parametro"s
func queryOverTarget(targetAddr string) []Parametro {
	db := aquireConn(targetAddr)
	sql := "select 'minha chave' as chave, 'meu valor' as valor"
	rs := []Parametro{}
	err := db.Select(&rs, sql)
	if err != nil {
		log.Fatal(err)
	}
	return rs
}
