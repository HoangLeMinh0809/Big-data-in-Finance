import { getProvinceName } from "../../utils/provinceMap";

function ProvinceFilter({ value, onChange, options }) {
  return (
    <select value={value} onChange={(e) => onChange(e.target.value)}>
      {options.map((province) => (
        <option key={province} value={province}>
          {province === "ALL" ? "Tất cả tỉnh/thành" : getProvinceName(province)}
        </option>
      ))}
    </select>
  );
}

export default ProvinceFilter;