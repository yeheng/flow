//! 会话挑战令牌：证明「连上这条通道的对端是我 spawn 的那个子进程」。
//!
//! 令牌是 **纵深防御**，不是安全边界：
//!
//! - 它防的是「本机另一个进程按名字/地址抢连」这类**意外**（脚本误连、
//!   端口扫描、另一个实例的残留客户端）。
//! - 它**不防**同 uid 的恶意进程：令牌经环境变量注入，同 uid 进程在
//!   Linux 上可以直接读 `/proc/<pid>/environ` 拿到它。
//!
//! unix 的 socketpair 路径**不需要令牌**（fd 本身就是能力：只有被
//! spawn 的子进程才继承得到），因此令牌只用在没有 FD 能力的承载上
//! （Windows named pipe / 回连路径）。
//!
//! 比较采用常数时间实现：令牌是握手门，naive 的 `==` 会把「比较耗时」
//! 变成侧信道（虽然现实威胁很低，但它是两行代码的事）。

use std::io;

/// 会话令牌：32 字节 CSPRNG 的 hex 编码。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Challenge(String);

impl Challenge {
    /// 令牌字节数。32 字节 = 256 bit，超出任何暴力猜测的可行范围。
    pub const BYTES: usize = 32;

    /// 生成一个新令牌。
    ///
    /// # Errors
    /// 取不到系统随机源时返回 I/O 错误（不回退到弱随机：令牌弱于随机数
    /// 等于没有令牌）。
    pub fn generate() -> io::Result<Self> {
        let mut bytes = [0u8; Self::BYTES];
        getrandom::fill(&mut bytes).map_err(io::Error::other)?;
        Ok(Self(hex::encode(bytes)))
    }

    /// 由已有字符串构造（用于子进程侧从环境变量读取）。
    pub fn from_raw(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// 令牌文本（hex）。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 校验对端出示的令牌。
    ///
    /// 常数时间比较：长度不同直接判否（长度本身不是秘密），长度相同则
    /// 逐字节异或累积。
    pub fn verify(&self, presented: &str) -> bool {
        let expected = self.0.as_bytes();
        let actual = presented.as_bytes();
        if expected.len() != actual.len() {
            return false;
        }
        let mut diff = 0u8;
        for (a, b) in expected.iter().zip(actual.iter()) {
            diff |= a ^ b;
        }
        diff == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_challenges_are_unique_and_hex() {
        let a = Challenge::generate().unwrap();
        let b = Challenge::generate().unwrap();
        assert_ne!(a, b);
        assert_eq!(a.as_str().len(), Challenge::BYTES * 2);
        assert!(a.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn verify_accepts_only_the_exact_token() {
        let token = Challenge::generate().unwrap();
        assert!(token.verify(token.as_str()));
        assert!(!token.verify(""));
        assert!(!token.verify(&token.as_str()[..token.as_str().len() - 1]));
        let mut flipped = token.as_str().to_string();
        let first = flipped.remove(0);
        let replacement = if first == '0' { '1' } else { '0' };
        flipped.insert(0, replacement);
        assert!(!token.verify(&flipped));
    }
}
