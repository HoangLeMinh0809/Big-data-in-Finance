export const PROVINCE_MAP = {
  ha_noi: "Hà Nội",
  hai_phong: "Hải Phòng",
  quang_ninh: "Quảng Ninh",
  thai_nguyen: "Thái Nguyên",
  bac_ninh: "Bắc Ninh",
  lao_cai: "Lào Cai",

  thanh_hoa: "Thanh Hóa",
  nghe_an: "Nghệ An",
  ha_tinh: "Hà Tĩnh",

  da_nang: "Đà Nẵng",
  thua_thien_hue: "Thừa Thiên Huế",
  quang_nam: "Quảng Nam",
  quang_ngai: "Quảng Ngãi",
  binh_dinh: "Bình Định",
  khanh_hoa: "Khánh Hòa",

  dak_lak: "Đắk Lắk",
  lam_dong: "Lâm Đồng",

  ho_chi_minh: "TP. Hồ Chí Minh",
  binh_duong: "Bình Dương",
  dong_nai: "Đồng Nai",
  ba_ria_vung_tau: "Bà Rịa - Vũng Tàu",
  tay_ninh: "Tây Ninh",

  long_an: "Long An",
  tien_giang: "Tiền Giang",
  can_tho: "Cần Thơ",
  an_giang: "An Giang",
  kien_giang: "Kiên Giang",
  ca_mau: "Cà Mau",
};

export function getProvinceName(key) {
  return PROVINCE_MAP[key] || key;
}