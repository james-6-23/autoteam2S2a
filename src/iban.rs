use std::sync::Mutex;

use fake::Fake;
use fake::faker::address::en::{CityName, StreetName};
use rand::Rng;

/// 德国 BLZ (Bankleitzahl) 列表 — 仅大型可靠银行
const DEFAULT_BLZ_LIST: &[&str] = &[
    // Deutsche Bank (德意志银行)
    "10070000", "10070024", "20070000", "30070000", "50070000", "60070000", "70070000", "80070000",
    // Commerzbank (德商银行)
    "10040000", "20040000", "30040000", "50040000", "60040000", "70040000", "80040000",
    // Sparkasse (储蓄银行)
    "10050000", "20050000", "30050000", "50050000", "60050000", "70050000", "80050000",
    // Postbank (邮政银行)
    "10010010", "20010020", "30010400", "50010060", "60010070", // ING-DiBa
    "50010517", // DKB
    "12030000", // HypoVereinsbank / UniCredit
    "70020270", "10020890", "20020090", "30020900", "50020700", "60020290",
    // Targobank
    "10020900", "20020200", // Volksbank
    "10090000", "20090500", "30060601", "50060400", "60090100", "70090100",
];

/// 德国 IBAN 生成器
///
/// 使用 ISO 7064 Mod 97-10 校验位算法生成合法的 DE IBAN。
/// BLZ 列表支持运行时动态移除（如某个 BLZ 导致支付报错 bank_account_unusable）。
pub struct GermanIbanGenerator {
    blz_list: Mutex<Vec<String>>,
    last_blz: Mutex<String>,
}

impl GermanIbanGenerator {
    pub fn new() -> Self {
        let list: Vec<String> = DEFAULT_BLZ_LIST.iter().map(|s| s.to_string()).collect();
        Self {
            blz_list: Mutex::new(list),
            last_blz: Mutex::new(String::new()),
        }
    }

    /// 生成一个随机的德国 IBAN
    pub fn generate(&self) -> String {
        let mut rng = rand::rng();

        // 选择随机 BLZ
        let blz = {
            let list = self.blz_list.lock().unwrap();
            if list.is_empty() {
                // 如果全部移除了就重新填充
                drop(list);
                self.reset();
                let list = self.blz_list.lock().unwrap();
                list[rng.random_range(0..list.len())].clone()
            } else {
                list[rng.random_range(0..list.len())].clone()
            }
        };

        // 记录使用的 BLZ
        {
            let mut last = self.last_blz.lock().unwrap();
            *last = blz.clone();
        }

        // 生成 10 位随机账号
        let account_number: String = (0..10)
            .map(|_| rng.random_range(0u8..10))
            .map(|d| (b'0' + d) as char)
            .collect();

        // BBAN = BLZ + AccountNumber (18位)
        let bban = format!("{blz}{account_number}");

        // 计算 IBAN 校验位 (ISO 7064 Mod 97-10)
        // 1. 将 "DE00" + BBAN 中的字母转为数字 (D=13, E=14)
        // 2. 将 BBAN + "131400" 做 mod 97
        // 3. 校验位 = 98 - (上述结果)
        let check_string = format!("{bban}131400"); // BBAN + D(13)E(14)00
        let check_digits = 98 - mod97(&check_string);

        format!("DE{check_digits:02}{bban}")
    }

    /// 获取生成最后一次使用的 BLZ
    pub fn last_blz(&self) -> String {
        self.last_blz.lock().unwrap().clone()
    }

    /// 移除指定的 BLZ（支付失败时调用）
    pub fn remove_blz(&self, blz: &str) {
        let mut list = self.blz_list.lock().unwrap();
        list.retain(|b| b != blz);
    }

    /// 重新填充 BLZ 列表
    fn reset(&self) {
        let mut list = self.blz_list.lock().unwrap();
        *list = DEFAULT_BLZ_LIST.iter().map(|s| s.to_string()).collect();
    }
}

/// 对超长数字字符串计算 mod 97 (ISO 7064)
fn mod97(s: &str) -> u64 {
    let mut remainder: u64 = 0;
    for ch in s.chars() {
        let digit = ch as u64 - b'0' as u64;
        remainder = (remainder * 10 + digit) % 97;
    }
    remainder
}

/// 使用 fake 动态生成德国账单地址（街道, 邮编, 城市）
pub fn random_german_address() -> (String, String, String) {
    let mut street: String = StreetName().fake();
    if !street.chars().any(|c| c.is_ascii_digit()) {
        let no = rand::rng().random_range(1..=220);
        street = format!("{street} {no}");
    }
    let postal_code = format!("{:05}", rand::rng().random_range(10000..=99999));
    let city: String = CityName().fake();
    (street, postal_code, city)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_iban_is_valid() {
        let generator = GermanIbanGenerator::new();
        for _ in 0..20 {
            let iban = generator.generate();
            assert!(iban.starts_with("DE"), "IBAN 应以 DE 开头: {iban}");
            assert_eq!(iban.len(), 22, "德国 IBAN 应为 22 位: {iban}");

            // 验证校验位：移动前4位到末尾，字母换数字，mod 97 == 1
            let rearranged = format!("{}{}", &iban[4..], &iban[..4]);
            let numeric: String = rearranged
                .chars()
                .flat_map(|c| {
                    if c.is_ascii_digit() {
                        vec![c]
                    } else {
                        let n = c as u32 - 'A' as u32 + 10;
                        n.to_string().chars().collect()
                    }
                })
                .collect();
            assert_eq!(mod97(&numeric), 1, "IBAN 校验失败: {iban}");
        }
    }

    #[test]
    fn remove_blz_works() {
        let generator = GermanIbanGenerator::new();
        let blz = "10070000";
        generator.remove_blz(blz);
        let list = generator.blz_list.lock().unwrap();
        assert!(!list.contains(&blz.to_string()));
    }
}
