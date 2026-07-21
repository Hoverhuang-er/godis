# Godis v1.3.1

![license](https://img.shields.io/github/license/Hoverhuang-er/godis)
[![Build Status](https://github.com/Hoverhuang-er/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/Hoverhuang-er/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/Hoverhuang-er/godis/badge.svg?branch=master)](https://coveralls.io/github/Hoverhuang-er/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Hoverhuang-er/godis)](https://goreportcard.com/report/github.com/Hoverhuang-er/godis)
[![Go Reference](https://pkg.go.dev/badge/github.com/Hoverhuang-er/godis.svg)](https://pkg.go.dev/github.com/Hoverhuang-er/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[English](https://github.com/Hoverhuang-er/godis/blob/master/docs/README.md) | [中文版](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_CN.md) | [日本語](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_JA.md)

`Godis` on Go-kielellä toteutettu Redis-palvelin. Sen tarkoituksena on tarjota esimerkki korkean suorituskyvyn middlewaren kirjoittamisesta Go-ohjelmointikielellä.

Keskeiset ominaisuudet:

- Redis 8.8.0 -komentoyhteensopivuus
- Tuki string-, list-, hash-, set-, sorted set- ja bitmap-tietorakenteille
- RediSearch (FT.CREATE, FT.SEARCH, FT.DROPINDEX jne.)
- Time Series (TS.CREATE, TS.ADD, TS.GET, TS.RANGE jne.)
- Redis-Vector (VECTOR-kenttätyyppi, KNN-haku)
- Rinnakkaisydin parempaa suorituskykyä varten
- TTL (automaattinen vanheneminen)
- Julkaisu/tilaus (Publish/Subscribe)
- GEO-paikannus
- AOF ja AOF-uudelleenkirjoitus
- RDB:n luku ja kirjoitus
- Useita tietokantoja ja `SELECT`-komento
- Transaktiot ovat **atomisia** ja eristettyjä. Jos virheitä ilmenee suorituksen aikana, godis peruuttaa suoritetut komennot
- Replikointi
- Palvelinpuolen klusteri, joka on läpinäkyvä asiakkaalle. Voit yhdistää mihin tahansa klusterin solmuun päästäksesi käsiksi kaikkiin klusterin tietoihin
  - Raft-pohjainen klusterimetadatan hallinta. Tukee dynaamista laajennusta, uudelleenbalansointia ja vikasietoisuutta
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX` -komennot suoritetaan atomisesti klusteritilassa, ja ne tukevat avaimia useiden solmujen yli
  - `MULTI`-transaktiot ovat tuettuja slotin sisällä klusteritilassa

Lisätietoja on [kehittajan blogissa](https://www.cnblogs.com/Finley/category/1598973.html) (kiinaksi).

## Aloitus

Voit ladata suoritettavan ohjelman taman repositorion julkaisusivulta (tukee Linux- ja Darwin-jarjestelmia).

```bash
./godis-darwin
```

```bash
./godis-linux
```

![](https://i.loli.net/2021/05/15/oQM1yZ6pWm3AIEj.png)

Voit kayttaa redis-cli:a tai muuta Redis-asiakasta yhdistaaksesi godis-palvelimeen, joka kuuntelee oletuksena osoitetta 0.0.0.0:6399.

![](https://i.loli.net/2021/05/15/7WquEgonzY62sZI.png)

Ohjelma yrittaa lukea konfiguraatiotiedoston polun `CONFIG`-ymparistomuuttujasta.

Jos ymparistomuuttujaa ei ole asetettu, ohjelma yrittaa lukea `standalone.toml`-tiedostoa (tai `redis.conf`) tyohakemistosta.

Katso kaikki konfiguraatiotiedot tiedostoista [standalone.toml](./standalone.toml) ja [example.conf](./example.conf).

### Klusteritila

Tarjoamme node1.conf ja node2.conf -tiedostot demonstraatiota varten. Kayta seuraavaa komentorivia kaynnistaaksesi kahden solmun klusterin:

```bash
CONFIG=node1.conf ./godis-darwin &
CONFIG=node2.conf ./godis-darwin &
```

Yhdista mihin tahansa klusterin solmuun paastyaksesi käsiksi kaikkiin klusterin tietoihin:

```cmd
redis-cli -p 6399
```

Klusterikonfiguraatiota varten katso [example.conf](./example.conf).

### Prometheus-seuranta

Godis tarjoaa Prometheus-yhteensopivat mittarit `/metrics`-osoitteessa portissa `9121` (muokattavissa `monitoring.prometheus_port`-asetuksella `standalone.toml`-tiedostossa). Mittarit ovat **kaytossa oletuksena** ja noudattavat `redis_exporter`-nimeamiskaytantoja yhteensopivuuden varmistamiseksi olemassa olevien Redis-mittausnayttojen kanssa.

```bash
# Oletusarvoinen scrappausosoite
curl http://localhost:9121/metrics
```

**Keskeiset mittarit:**
- `godis_connected_clients` — nykyiset aktiiviset yhteydet
- `godis_commands_total` — kasitellyt komennot yhteensa
- `godis_keyspace_hits_total` / `godis_keyspace_misses_total` — vaLIMuistiosumien/ohitusten maara
- `godis_db_keys` — avainten maara tietokannoittain
- `godis_db_avg_ttl_seconds` — keskimaarainen TTL tietokannoittain
- `godis_slowlog_length` — hitaiden kyselyjen lokin pituus
- Kuumien ja suurten avainten tunnistus (säännöllinen näytteenotto)

Poistaaksesi mittarit kaytosta, aseta `prometheus_enabled = false` konfiguraatiotiedoston `[monitoring]`-osiossa. Kaikki seuranta-asetukset voi ladata uudelleen ajon aikana.

```toml
[monitoring]
prometheus_enabled = true
prometheus_port = 9121
```

## Rueidis-asiakasesimerkki

[Rueidis](https://github.com/redis/rueidis) on korkean suorituskyvyn Redis-asiakas Go:lle. Nain sita kaytetaan Godisin kanssa:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/rueidis"
)

func main() {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"localhost:6399"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// SET/GET-esimerkki
	err = client.Do(ctx, client.B().Set().Key("foo").Value("bar").Build()).Error()
	if err != nil {
		log.Fatal(err)
	}

	val, err := client.Do(ctx, client.B().Get().Key("foo").Build()).ToString()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GET foo = %s\n", val)

	// RediSearch-esimerkki
	// Vaatii FT.CREATE-indeksin luomisen ensin
	result, err := client.Do(ctx, client.B().FtSearch().Index("idx").Query("@field:val").Build()).ToArray()
	if err != nil {
		log.Printf("Hakuhuomautus: %v (luo indeksi ensin FT.CREATE:lla)", err)
	}
	_ = result

	// Time Series -esimerkki
	err = client.Do(ctx, client.B().TsAdd().Key("ts:temp").Timestamp(1).Value(25.5).Build()).Error()
	if err != nil {
		log.Printf("Aikasarjahuomautus: %v", err)
	}
}
```

## Tuetut komennot

Katso [commands.md](https://github.com/Hoverhuang-er/godis/blob/master/commands.md).

## Suorituskykymittaus

Ymparisto:

Go version: 1.23
System: MacOS Monterey 12.5 M2 Air

redis-benchmarkin suorituskykyraportti:

```
PING_INLINE: 179211.45 requests per second, p50=1.031 msec                    
PING_MBULK: 173611.12 requests per second, p50=1.071 msec                    
SET: 158478.61 requests per second, p50=1.535 msec                    
GET: 156985.86 requests per second, p50=1.127 msec                    
INCR: 164473.69 requests per second, p50=1.063 msec                    
LPUSH: 151285.92 requests per second, p50=1.079 msec                    
RPUSH: 176678.45 requests per second, p50=1.023 msec                    
LPOP: 177619.89 requests per second, p50=1.039 msec                    
RPOP: 172413.80 requests per second, p50=1.039 msec                    
SADD: 159489.64 requests per second, p50=1.047 msec                    
HSET: 175131.36 requests per second, p50=1.031 msec                    
SPOP: 170648.45 requests per second, p50=1.031 msec                    
ZADD: 165289.25 requests per second, p50=1.039 msec                    
ZPOPMIN: 185528.77 requests per second, p50=0.999 msec                    
LPUSH (needed to benchmark LRANGE): 172117.05 requests per second, p50=1.055 msec                    
LRANGE_100 (first 100 elements): 46511.62 requests per second, p50=4.063 msec                   
LRANGE_300 (first 300 elements): 21217.91 requests per second, p50=9.311 msec                     
LRANGE_500 (first 500 elements): 13331.56 requests per second, p50=14.407 msec                    
LRANGE_600 (first 600 elements): 11153.25 requests per second, p50=17.007 msec                    
MSET (10 keys): 88417.33 requests per second, p50=3.687 msec  
```

## Koodin lukuopas

Projekti noudattaa [Go Project Layout](https://github.com/golang-standards/project-layout) -standardia:

```
godis/
├── cmd/                          # Saantopisteet
│   ├── godis/main.go             # Godis-palvelin (standalone/cluster)
│   ├── godis/cli.go              # Sisaanrakennettu redis-cli (--cli)
│   └── operator/main.go          # Kubernetes Operator
├── internal/                     # Yksityinen sovelluskoodi
│   ├── config/                   # TOML-konfiguraatio (viper, hot-reload)
│   ├── tcp/                      # TCP-palvelin (goroutine/yhteys)
│   ├── redis/                    # Redis-protokolla
│   │   ├── parser/               # RESP2/RESP3 -jasennin
│   │   ├── protocol/             # Vastustyypit (Bulk, MultiBulk, Error jne.)
│   │   ├── server/               # Palvelinadapterit
│   │   │   ├── std/              #   Vakio net.TCP
│   │   │   └── gnet/             #   gnet tapahtumasilmukka
│   │   ├── client/               # Solmujen valinen asiakas
│   │   └── connection/           # Yhteyden tila (DB, auth, multi)
│   ├── interface/                # Syden rajapintojen maaritykset
│   ├── database/                 # Tallemusmoottori ja komentokasittelijat
│   │   ├── server.go             # Monitietokantapalvelin
│   │   ├── database.go           # Yksittainen tietokanta
│   │   ├── router.go             # Komentotaulu ja reititys
│   │   ├── string.go             # GET, SET, INCR, APPEND jne.
│   │   ├── hash.go               # HSET, HGET, HDEL jne.
│   │   ├── list.go               # LPUSH, LRANGE, LINDEX jne.
│   │   ├── set.go                # SADD, SMEMBERS, SINTER jne.
│   │   ├── sortedset.go          # ZADD, ZRANGE, ZRANK jne.
│   │   ├── stream.go             # XADD, XREAD, XGROUP jne.
│   │   ├── geo.go                # GEOADD, GEOSEARCH jne.
│   │   ├── keys.go               # DEL, EXISTS, EXPIRE, TTL jne.
│   │   ├── transaction.go        # MULTI, EXEC, WATCH
│   │   ├── persistence.go        # RDB-lataus
│   │   ├── timeseries.go         # TS.CREATE, TS.ADD, TS.GET, TS.RANGE
│   │   ├── search.go             # FT.CREATE, FT.SEARCH, FT.DROPINDEX
│   │   ├── json.go               # JSON.SET, JSON.GET, JSON.DEL jne.
│   │   ├── bloom.go              # BF.ADD, BF.EXISTS, BF.RESERVE
│   │   ├── hyperloglog.go        # PFADD, PFCOUNT, PFMERGE
│   │   ├── topk.go               # TOPK.ADD, TOPK.QUERY, TOPK.LIST
│   │   ├── cms.go                # CMS.INCRBY, CMS.QUERY
│   │   ├── tdigest.go            # TDIGEST.ADD, TDIGEST.QUANTILE
│   │   ├── bitfield.go           # BITFIELD, BITFIELD_RO
│   │   └── array.go              # AR.SET, AR.GET, AR.APPEND
│   ├── aof/                      # AOF-pysyvyys ja uudelleenkirjoitus
│   ├── pubsub/                   # Julkaisu/tilaus
│   ├── cluster/                  # Klusteritila
│   │   ├── core/                 # Slot-reititys, TCC-tapahtumat
│   │   ├── commands/             # Klusteritietoiset komennot
│   │   └── raft/                 # Raft-konsensus
│   ├── monitoring/               # Prometheus-metriikat
│   ├── auth/entraid/             # Entra ID JWT -vahvistus (Azure AD)
│   ├── datastruct/               # Tietorakenteiden toteutukset
│   │   ├── dict/                 # Rinnakkais hajautustaulu
│   │   ├── list/                 # Quicklist
│   │   ├── set/                  # Hajautustauluun perustuva joukko
│   │   ├── sortedset/            # Skippilista
│   │   ├── bitmap/               # Bittitaulukko
│   │   ├── stream/               # Radix-puu -pohjainen virta
│   │   ├── search/               # Kaanteisindeksi
│   │   ├── hyperloglog/          # Kardiaalisuuden arviointi
│   │   ├── bloom/                # Bloom-suodatin
│   │   ├── cms/                  # Count-min Sketch
│   │   ├── topk/                 # Top-K useimmat
│   │   ├── tdigest/              # T-Digest (kvantiilit)
│   │   ├── timeseries/           # Aikasarjat
│   │   ├── array/                # Sparse indeksi-taulukko
│   │   └── lock/                 # Avaintason luku/kirjoituslukot
│   └── lib/                      # Apukirjastot
│       ├── logger/               # Rakenteinen lokitus
│       ├── pool/                 # Yleinen objektipooli
│       ├── timewheel/            # Aikapyorä (vanheneminen ja cron)
│       ├── wildcard/             # Globbaus
│       ├── consistenthash/       # Johdonmukainen hajautusrengas
│       ├── idgenerator/          # Snowflake ID -generaattori
│       ├── arena/                # Muistiallocated
│       └── greenteagc/           # GC-viritys
├── config/                       # Konfiguraatiotiedostot
│   ├── standalone.toml           # Standalone-konfiguraatio
│   ├── cluster.toml              # Klusterikonfiguraatio
│   └── crd/                      # Kubernetes CRD
├── charts/                       # Helm Chart
└── patches/                      # Korjattu riippuvuus
```

### Ehdotettu luku jarjestys

Aloita **saantopisteesta** (`cmd/godis/main.go`) ja seuraa tietovirtaa:

1. **`internal/config/`** — konfiguraation lataus ja hot-reload
2. **`internal/tcp/`** + **`internal/redis/parser/`** — yhteyksien hallinta ja RESP-jasennys
3. **`internal/database/`** — sydin: reititys → monitietokanta → komennot
4. **`internal/datastruct/`** — tietorakenteet (dict, skiplist, quicklist jne.)
5. **`internal/aof/`** + **`internal/database/persistence.go`** — pysyvyys
6. **`internal/cluster/`** — hajautettu tila: Raft, slot-reititys, TCC
7. **`internal/monitoring/`** — havainnoitavuus: Prometheus

# Lisenssi

Tama projekti on lisensoitu [GPL-lisenssilla](https://github.com/Hoverhuang-er/godis/blob/master/LICENSE).
