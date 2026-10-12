// Command seed 给本地开发环境灌测试数据：10 个用户 + 44 个商品（5 大分类）。

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/redis/go-redis/v9"

	"go-ecom-admin/pkg/cache"
	"go-ecom-admin/pkg/config"
	"go-ecom-admin/pkg/database"

	productmodel "go-ecom-admin/internal/product/model"
	productrepo "go-ecom-admin/internal/product/repository"
	usermodel "go-ecom-admin/internal/user/model"
)

var seedAddresses = []usermodel.Address{
	{ReceiverName: "张伟", Phone: "13800138001", Address: "北京市朝阳区望京街道望京 SOHO T1 座 1802"},
	{ReceiverName: "李娜", Phone: "13900139002", Address: "上海市浦东新区张江路 605 号 3 号楼 501"},
	{ReceiverName: "王芳", Phone: "15000150003", Address: "广州市天河区科韵路 16 号天河软件园 8 层"},
	{ReceiverName: "刘洋", Phone: "18600186004", Address: "深圳市南山区科技园南区深南大道 9988 号"},
	{ReceiverName: "陈静", Phone: "13700137005", Address: "杭州市西湖区文三路 90 号东部软件园 2 幢"},
	{ReceiverName: "杨帆", Phone: "15800158006", Address: "成都市高新区天府大道北段 1700 号新世纪广场"},
	{ReceiverName: "赵磊", Phone: "18900189007", Address: "武汉市洪山区光谷大道 77 号光谷软件园 E 城"},
	{ReceiverName: "孙悦", Phone: "13600136008", Address: "南京市雨花台区软件大道 109 号软件谷科创城"},
}

// seedPassword 是所有种子用户统一使用的明文密码，仅用于本地开发/联调登录

const seedPassword = "123456"

type seedProduct struct {
	name        string
	description string
	imageURL    string
	category    string
	priceMin    int64 // 价格区间下限（分），含
	priceMax    int64 // 价格区间上限（分），含
}

// productNames 用真实商品名而不是 "product-1" 这种占位符，是为了让本地联调时
// 看到的列表页/详情页尽量接近真实产品的观感，便于顺带发现前端展示上的问题
// （比如商品名过长导致的排版截断）。
var seedProducts = []seedProduct{
	// —— 手机数码（原 20 个，价格为 19.9~299 元区间的数码小件）——
	{"蓝牙耳机", "蓝牙 5.3 真无线耳机，支持主动降噪，单次续航 8 小时，配充电盒可达 32 小时。", "https://picsum.photos/seed/bluetooth-earbuds/400/300", "手机数码", 1990, 29900},
	{"机械键盘", "87 键热插拔机械键盘，Gasket 结构，三模连接，RGB 背光全键无冲。", "https://picsum.photos/seed/mechanical-keyboard/400/300", "手机数码", 1990, 29900},
	{"USB-C充电器", "65W 氮化镓充电器，双 C 口快充，兼容手机耳机笔记本，折叠插脚便携。", "https://picsum.photos/seed/usb-c-charger/400/300", "手机数码", 1990, 29900},
	{"无线鼠标", "2.4G/蓝牙双模无线鼠标，静音微动，人体工学握感，一节电池用半年。", "https://picsum.photos/seed/wireless-mouse/400/300", "手机数码", 1990, 29900},
	{"27英寸显示器", "27 英寸 2K 165Hz IPS 显示器，95% DCI-P3 色域，升降旋转支架，办公游戏两相宜。", "https://picsum.photos/seed/monitor-27/400/300", "手机数码", 1990, 29900},
	{"移动电源", "20000mAh 大容量移动电源，22.5W 双向快充，数显电量，可上飞机。", "https://picsum.photos/seed/power-bank/400/300", "手机数码", 1990, 29900},
	{"智能手表", "1.43 英寸 AMOLED 智能手表，血氧心率监测，多种健身模式，14 天长续航。", "https://picsum.photos/seed/smart-watch/400/300", "手机数码", 1990, 29900},
	{"蓝牙音箱", "便携蓝牙音箱，IPX7 防水，360° 环绕出声，露营浴室都能用，续航 24 小时。", "https://picsum.photos/seed/bluetooth-speaker/400/300", "手机数码", 1990, 29900},
	{"笔记本支架", "铝合金笔记本支架，六档高度调节，镂空散热，折叠后仅一本书厚。", "https://picsum.photos/seed/laptop-stand/400/300", "手机数码", 1990, 29900},
	{"人体工学椅", "人体工学电脑椅，4D 扶手 + 腰托独立调节，全网透气，久坐办公舒缓腰背。", "https://picsum.photos/seed/ergonomic-chair/400/300", "手机数码", 1990, 29900},
	{"高清摄像头", "2K 高清摄像头，自动对焦 + 双降噪麦克风，网课视频会议画质清晰。", "https://picsum.photos/seed/hd-webcam/400/300", "手机数码", 1990, 29900},
	{"无线充电器", "15W 磁吸无线充电器，手机耳机都能充，带散热风扇，夜间可当支架。", "https://picsum.photos/seed/wireless-charger/400/300", "手机数码", 1990, 29900},
	{"游戏手柄", "多平台游戏手柄，霍尔摇杆永不漂移，六轴体感，PC/Switch/手机通吃。", "https://picsum.photos/seed/game-controller/400/300", "手机数码", 1990, 29900},
	{"降噪耳机", "头戴式主动降噪耳机，-45dB 深度降噪，40mm 大动圈，通勤出差隔绝喧嚣。", "https://picsum.photos/seed/anc-headphones/400/300", "手机数码", 1990, 29900},
	{"机械硬盘", "4TB 企业级机械硬盘，7200 转 CMR 垂直记录，NAS 仓储备份大容量之选。", "https://picsum.photos/seed/hdd-4tb/400/300", "手机数码", 1990, 29900},
	{"固态硬盘", "1TB NVMe 固态硬盘，PCIe 4.0 读速 7000MB/s，带独立缓存，游戏秒加载。", "https://picsum.photos/seed/nvme-ssd/400/300", "手机数码", 1990, 29900},
	{"千兆路由器", "Wi-Fi 6 千兆路由器，四天线穿墙，Mesh 组网支持，全屋信号无死角。", "https://picsum.photos/seed/wifi6-router/400/300", "手机数码", 1990, 29900},
	{"网络交换机", "8 口千兆网络交换机，即插即用免配置，金属外壳散热好，小型办公室适用。", "https://picsum.photos/seed/network-switch/400/300", "手机数码", 1990, 29900},
	{"显卡散热器", "显卡散热伴侣，双风扇下压式辅助散热，RGB 神光同步，降温降噪两不误。", "https://picsum.photos/seed/gpu-cooler/400/300", "手机数码", 1990, 29900},
	{"电竞椅垫", "电竞椅专用坐垫靠垫套装，记忆棉慢回弹，久坐不累，适配大多数椅子。", "https://picsum.photos/seed/chair-cushion/400/300", "手机数码", 1990, 29900},

	// —— 家用电器（99~499 元区间）——
	{"变频空调", "1.5 匹新一级能效变频空调，冷暖两用，静音运行，WiFi 远程控制。", "https://picsum.photos/seed/inverter-ac/400/300", "家用电器", 9900, 49900},
	{"滚筒洗衣机", "10 公斤滚筒洗衣机，巴氏除菌洗，变频电机，超薄可嵌入式安装。", "https://picsum.photos/seed/washing-machine/400/300", "家用电器", 9900, 49900},
	{"扫地机器人", "激光导航扫地机器人，扫拖一体，自动集尘，APP 建图规划路线。", "https://picsum.photos/seed/robot-vacuum/400/300", "家用电器", 9900, 49900},
	{"智能电饭煲", "4L 智能电饭煲，IH 电磁加热，24 小时预约，不粘内胆好清洗。", "https://picsum.photos/seed/rice-cooker/400/300", "家用电器", 9900, 49900},
	{"空气炸锅", "大容量空气炸锅，360° 热风循环，无油低脂更健康，触控面板好上手。", "https://picsum.photos/seed/air-fryer/400/300", "家用电器", 9900, 49900},
	{"反渗透净水器", "厨下式反渗透净水器，五级过滤，无桶大流量，出水直饮更安心。", "https://picsum.photos/seed/water-purifier/400/300", "家用电器", 9900, 49900},

	// —— 运动户外（29~129 元区间）——
	{"缓震跑鞋", "缓震跑步鞋，透气网面，防滑橡胶大底，日常慢跑训练皆宜。", "https://picsum.photos/seed/running-shoes/400/300", "运动户外", 2900, 12900},
	{"防滑瑜伽垫", "加厚防滑瑜伽垫，TPE 环保材质，纹理抓地，附收纳网袋。", "https://picsum.photos/seed/yoga-mat/400/300", "运动户外", 2900, 12900},
	{"双人帐篷", "双人双层帐篷，防雨防晒，铝合金杆轻量耐用，公园露营说走就走。", "https://picsum.photos/seed/camping-tent/400/300", "运动户外", 2900, 12900},
	{"骑行头盔", "一体成型骑行头盔，PC 外壳 EPS 缓冲，多孔通风，夜骑反光条。", "https://picsum.photos/seed/bike-helmet/400/300", "运动户外", 2900, 12900},
	{"钢丝跳绳", "钢丝轴承跳绳，计数手柄，长度可调节，燃脂塑形好帮手。", "https://picsum.photos/seed/jump-rope/400/300", "运动户外", 2900, 12900},
	{"登山背包", "40L 登山背包，人体工学背负系统，防泼水面料，多仓分区收纳。", "https://picsum.photos/seed/hiking-backpack/400/300", "运动户外", 2900, 12900},

	// —— 美妆个护（3.9~39.9 元区间）——
	{"氨基酸洗面奶", "氨基酸温和洗面奶，绵密泡沫，深层清洁不紧绷，敏感肌可用。", "https://picsum.photos/seed/facial-cleanser/400/300", "美妆个护", 390, 3990},
	{"物理防晒霜", "SPF50+ 物理防晒霜，清爽不油腻，成膜快，通勤旅行都适用。", "https://picsum.photos/seed/sunscreen/400/300", "美妆个护", 390, 3990},
	{"声波电动牙刷", "声波电动牙刷，五种模式，2 分钟智能定时，无线充电底座。", "https://picsum.photos/seed/sonic-toothbrush/400/300", "美妆个护", 390, 3990},
	{"玻尿酸面霜", "玻尿酸保湿面霜，质地清爽，深层补水，干皮油皮都友好。", "https://picsum.photos/seed/face-cream/400/300", "美妆个护", 390, 3990},
	{"无硅油洗发水", "无硅油洗发水，控油蓬松，氨基酸配方，留香持久不扁塌。", "https://picsum.photos/seed/shampoo/400/300", "美妆个护", 390, 3990},
	{"木质调香水", "木质调淡香水，前调柑橘中调茉莉尾调雪松，日常通勤百搭。", "https://picsum.photos/seed/perfume/400/300", "美妆个护", 390, 3990},

	// —— 休闲零食（9.9~69.9 元区间）——
	{"每日坚果", "混合坚果礼盒，每日一包科学配比，孕妇儿童皆宜。", "https://picsum.photos/seed/daily-nuts/400/300", "休闲零食", 990, 6990},
	{"原切薯片", "原切薯片，非油炸烘焙工艺，轻盐海苔味，咔嚓一口停不下来。", "https://picsum.photos/seed/potato-chips/400/300", "休闲零食", 990, 6990},
	{"黑巧克力礼盒", "黑巧克力礼盒，72% 可可含量，低糖配方，丝滑不腻口。", "https://picsum.photos/seed/dark-chocolate/400/300", "休闲零食", 990, 6990},
	{"精品咖啡豆", "中度烘焙咖啡豆，耶加雪菲产区，柑橘花香，下单现磨现发。", "https://picsum.photos/seed/coffee-beans/400/300", "休闲零食", 990, 6990},
	{"麻辣辣条大礼包", "麻辣辣条大礼包，怀旧口味，独立小包，解馋不脏手。", "https://picsum.photos/seed/spicy-strips/400/300", "休闲零食", 990, 6990},
	{"白桃味果冻", "蒟蒻果冻，白桃味，零卡代糖，Q 弹爽滑，开口即食。", "https://picsum.photos/seed/peach-jelly/400/300", "休闲零食", 990, 6990},
}

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	// 带退避重试的连接
	db, err := database.ConnectWithRetry(context.Background(), func() (*gorm.DB, error) {
		return gorm.Open(mysql.Open(cfg.MySQL.DSN()), &gorm.Config{TranslateError: true})
	}, database.ConnectConfig{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect mysql: %v\n", err)
		os.Exit(1)
	}

	// 带锁迁移：seed 可能和业务服务同时启动（如 make k8s-seed 在
	// Deployment 滚动起来时跑），同样要走命名锁避免抢建表。
	if err := database.Migrate(db, 30*time.Second, &usermodel.User{}, &usermodel.Address{}, &productmodel.Product{}); err != nil {
		fmt.Fprintf(os.Stderr, "auto migrate: %v\n", err)
		os.Exit(1)
	}

	if err := db.Exec("TRUNCATE TABLE users").Error; err != nil {
		fmt.Fprintf(os.Stderr, "truncate users: %v\n", err)
		os.Exit(1)
	}
	if err := db.Exec("TRUNCATE TABLE products").Error; err != nil {
		fmt.Fprintf(os.Stderr, "truncate products: %v\n", err)
		os.Exit(1)
	}
	// addresses（阶段 5B）是 mock 收货信息池，同样 TRUNCATE 保证确定初始状态。
	if err := db.Exec("TRUNCATE TABLE addresses").Error; err != nil {
		fmt.Fprintf(os.Stderr, "truncate addresses: %v\n", err)
		os.Exit(1)
	}
	// cart_items
	if err := db.Exec("TRUNCATE TABLE cart_items").Error; err != nil {
		fmt.Fprintf(os.Stderr, "truncate cart_items: %v\n", err)
		os.Exit(1)
	}

	// 清缓存是卫生措施不是核心职责，Redis 连不上只警告不中断
	// （比如本地裸跑 seed 而 Redis 没起时，库表重置仍然有用）。
	// rdb 保持打开到函数结束：商品灌完后还要用它重建布隆过滤器。
	var rdb *redis.Client
	if client, err := cache.New(cache.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB}); err != nil {
		fmt.Fprintf(os.Stderr, "warn: connect redis, cache not flushed: %v\n", err)
	} else {
		rdb = client
		defer func() { _ = rdb.Close() }()
		if err := rdb.FlushDB(context.Background()).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "warn: flush redis: %v\n", err)
		}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(seedPassword), bcrypt.DefaultCost)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hash password: %v\n", err)
		os.Exit(1)
	}

	users := make([]*usermodel.User, 0, 10)
	for i := 1; i <= 10; i++ {
		users = append(users, &usermodel.User{
			Username:     fmt.Sprintf("user%d", i),
			Email:        fmt.Sprintf("user%d@example.com", i),
			PasswordHash: string(hash),
		})
	}
	if err := db.Create(&users).Error; err != nil {
		fmt.Fprintf(os.Stderr, "insert users: %v\n", err)
		os.Exit(1)
	}

	products := make([]*productmodel.Product, 0, len(seedProducts))
	for i, sp := range seedProducts {
		products = append(products, &productmodel.Product{
			Name: sp.name,
			// 分类靠描述埋词，ES 搜"手机数码"
			// 命中这一段，前端点分类 = 搜分类名，不改后端契约。
			Description: sp.description + "分类：" + sp.category + "。",

			PriceCents: sp.priceMin + rand.Int63n(sp.priceMax-sp.priceMin+1),
			Stock:      int32(rand.Intn(501)),

			OwnerID:  users[i%len(users)].ID,
			ImageURL: sp.imageURL,
		})
	}
	if err := db.Create(&products).Error; err != nil {
		fmt.Fprintf(os.Stderr, "insert products: %v\n", err)
		os.Exit(1)
	}

	// 布隆过滤器随种子数据重建：上面 FlushDB 已清场，直接把新 id 全量
	// 灌入即可（不需要先 DEL）。否则商品详情的布隆闸门会把所有种子
	// 商品判成"一定不存在"，详情页全部 404。
	if rdb != nil {
		bloom := cache.NewBloomFilter(rdb, "bloom:product", 100000, 0.01)
		added := 0
		for _, p := range products {
			if err := bloom.Add(context.Background(), p.ID); err != nil {
				fmt.Fprintf(os.Stderr, "warn: bloom add product id=%d: %v\n", p.ID, err)
				continue
			}
			added++
		}
		fmt.Printf("bloom filter: %d/%d products added\n", added, len(products))
	}

	// 收货信息池
	addresses := make([]*usermodel.Address, 0, len(seedAddresses))
	for i := range seedAddresses {
		addresses = append(addresses, &seedAddresses[i])
	}
	if err := db.Create(&addresses).Error; err != nil {
		fmt.Fprintf(os.Stderr, "insert addresses: %v\n", err)
		os.Exit(1)
	}

	// 全量重建 ES 索引
	esSearcher, err := productrepo.NewESSearcher(cfg.Elasticsearch.Addr, cfg.Elasticsearch.Index, zap.NewExample())
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: connect elasticsearch, search index not rebuilt: %v\n", err)
	} else if err := esSearcher.Recreate(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "warn: recreate elasticsearch index: %v\n", err)
	} else {
		indexed := 0
		for _, p := range products {
			// 单条失败不中断全量灌入：记录下来继续，最后让计数说话。
			if err := esSearcher.Index(context.Background(), p); err != nil {
				fmt.Fprintf(os.Stderr, "warn: es index product id=%d: %v\n", p.ID, err)
				continue
			}
			indexed++
		}
		fmt.Printf("elasticsearch: index %q recreated, %d/%d products indexed\n",
			cfg.Elasticsearch.Index, indexed, len(products))
	}

	fmt.Println("seeded users:")
	for _, u := range users {
		fmt.Printf("  id=%d username=%s (password=%s)\n", u.ID, u.Username, seedPassword)
	}

	fmt.Println("seeded products:")
	for _, p := range products {
		// 描述末尾埋的分类词也打出来，方便肉眼核对每个分类的商品数。
		// 按 rune 截尾而不是字节：中文多字节按字节截会把字切成乱码。
		tail := []rune(p.Description)
		if len(tail) > 12 {
			tail = tail[len(tail)-12:]
		}
		fmt.Printf("  id=%d name=%s price=%.2f元 stock=%d owner_id=%d …%s\n",
			p.ID, p.Name, float64(p.PriceCents)/100, p.Stock, p.OwnerID, string(tail))
	}

	fmt.Printf("seeded addresses: %d rows (mock pool for checkout display)\n", len(addresses))
}
