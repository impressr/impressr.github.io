#!/usr/bin/env python3
"""
Script to fetch and analyze user evaluation data from Supabase.

Generates 3 clean CSV files:
- Form 1: Hardness ratings (user vs system) and CoT quality per user
- Form 2: Model scores per user
- Form 3: CoT quality ratings per user
"""

import requests
import csv
from datetime import datetime
from collections import defaultdict

# Supabase configuration
SUPABASE_URL = "https://oqfbijskgpfqhbonbjww.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9xZmJpanNrZ3BmcWhib25iand3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk5MzIxMTksImV4cCI6MjA4NTUwODExOX0.9lbI8zWpPRytiO5j__DeniQ73d0ouuvsj17RDFsS_4s"

def fetch_all_data():
    """Fetch all ratings data from Supabase."""
    headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
    }
    
    response = requests.get(
        f'{SUPABASE_URL}/rest/v1/ratings?select=*',
        headers=headers
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
    return response.json()

def analyze_form1(all_data):
    """
    Form 1 Analysis: Hardness ratings (user vs system) and CoT quality.
    Returns: List of dicts with columns:
      - user_id
      - hardness_1_avg (avg user rating for system hardness 1 cases)
      - hardness_2_avg
      - hardness_3_avg
      - hardness_4_avg
      - cot_quality_avg
    """
    # Collect data per user
    user_data = defaultdict(lambda: {
        '1': [], '2': [], '3': [], '4': [],
        'cot': []
    })
    
    for user in all_data:
        user_id = user['user_id']
        state = user['data']
        
        if 'data_quality' not in state:
            continue
        
        answers = state['data_quality'].get('answers', {})
        
        for case_id, answer in answers.items():
            system_hardness = str(answer.get('system_hardness', ''))
            user_hardness = answer.get('hardness')
            cot_quality = answer.get('cot_quality')
            
            if user_hardness and system_hardness in ['1', '2', '3', '4']:
                user_data[user_id][system_hardness].append(int(user_hardness))
            
            if cot_quality:
                user_data[user_id]['cot'].append(int(cot_quality))
    
    # Calculate results
    results = []
    
    # Overall aggregates across all users
    all_hardness = {'1': [], '2': [], '3': [], '4': []}
    all_cot = []
    
    for user_id, data in sorted(user_data.items()):
        row = {'user_id': user_id}
        
        for level in ['1', '2', '3', '4']:
            if data[level]:
                avg = sum(data[level]) / len(data[level])
                row[f'hardness_{level}_avg'] = round(avg, 2)
                all_hardness[level].extend(data[level])
            else:
                row[f'hardness_{level}_avg'] = None
        
        if data['cot']:
            avg_cot = sum(data['cot']) / len(data['cot'])
            row['cot_quality_avg'] = round(avg_cot, 2)
            all_cot.extend(data['cot'])
        else:
            row['cot_quality_avg'] = None
        
        results.append(row)
    
    # Add ALL_USERS summary row
    summary = {'user_id': 'ALL_USERS'}
    for level in ['1', '2', '3', '4']:
        if all_hardness[level]:
            avg = sum(all_hardness[level]) / len(all_hardness[level])
            summary[f'hardness_{level}_avg'] = round(avg, 2)
        else:
            summary[f'hardness_{level}_avg'] = None
    
    if all_cot:
        avg_cot = sum(all_cot) / len(all_cot)
        summary['cot_quality_avg'] = round(avg_cot, 2)
    else:
        summary['cot_quality_avg'] = None
    
    results.append(summary)
    
    return results

def analyze_form2(all_data):
    """
    Form 2 Analysis: Model scores per user.
    Returns: List of dicts with columns:
      - user_id
      - huatuo_avg
      - m1_avg
      - medreason_avg
      - qwen8b_zs_avg
      - qwen8b_nocot_avg
      - qwen8b_sft_avg
      - qwen8b_rl_avg
    """
    model_keys = ['huatuo', 'm1', 'medreason', 'qwen8b_zs', 'qwen8b_nocot', 'qwen8b_sft', 'qwen8b_rl']
    
    # Collect data per user
    user_data = defaultdict(lambda: {model: [] for model in model_keys})
    
    for user in all_data:
        user_id = user['user_id']
        state = user['data']
        
        if 'model_evaluation' not in state:
            continue
        
        datasets = state['model_evaluation'].get('datasets', {})
        
        for dataset_key, dataset_data in datasets.items():
            answers = dataset_data.get('answers', {})
            
            for case_id, answer in answers.items():
                model_scores = answer.get('model_scores', {})
                
                for model in model_keys:
                    if model in model_scores and model_scores[model]:
                        score = int(model_scores[model])
                        user_data[user_id][model].append(score)
    
    # Calculate results
    results = []
    
    # Overall aggregates
    all_scores = {model: [] for model in model_keys}
    
    for user_id, data in sorted(user_data.items()):
        row = {'user_id': user_id}
        
        for model in model_keys:
            if data[model]:
                avg = sum(data[model]) / len(data[model])
                row[f'{model}_avg'] = round(avg, 2)
                all_scores[model].extend(data[model])
            else:
                row[f'{model}_avg'] = None
        
        results.append(row)
    
    # Add ALL_USERS summary row
    summary = {'user_id': 'ALL_USERS'}
    for model in model_keys:
        if all_scores[model]:
            avg = sum(all_scores[model]) / len(all_scores[model])
            summary[f'{model}_avg'] = round(avg, 2)
        else:
            summary[f'{model}_avg'] = None
    
    results.append(summary)
    
    return results

def analyze_form3(all_data):
    """
    Form 3 Analysis: CoT quality ratings per user.
    Returns: List of dicts with columns:
      - user_id
      - cot_quality_avg
    """
    # Collect data per user
    user_data = defaultdict(list)
    
    for user in all_data:
        user_id = user['user_id']
        state = user['data']
        
        if 'cot_evaluation' not in state:
            continue
        
        answers = state['cot_evaluation'].get('answers', {})
        
        for case_id, answer in answers.items():
            quality = answer.get('quality')
            if quality:
                user_data[user_id].append(int(quality))
    
    # Calculate results
    results = []
    all_quality = []
    
    for user_id, scores in sorted(user_data.items()):
        if scores:
            avg = sum(scores) / len(scores)
            results.append({
                'user_id': user_id,
                'cot_quality_avg': round(avg, 2)
            })
            all_quality.extend(scores)
        else:
            results.append({
                'user_id': user_id,
                'cot_quality_avg': None
            })
    
    # Add ALL_USERS summary row
    if all_quality:
        avg = sum(all_quality) / len(all_quality)
        results.append({
            'user_id': 'ALL_USERS',
            'cot_quality_avg': round(avg, 2)
        })
    else:
        results.append({
            'user_id': 'ALL_USERS',
            'cot_quality_avg': None
        })
    
    return results

def main():
    print("🚀 Fetching data from Supabase...")
    
    try:
        all_data = fetch_all_data()
        print(f"✅ Retrieved data for {len(all_data)} users\n")
        
        if len(all_data) == 0:
            print("⚠️  No data found in Supabase. Please ensure users have submitted evaluations.")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Form 1: Hardness ratings and CoT quality
        print("📊 Analyzing Form 1 (Data Quality Assessment)...")
        form1_results = analyze_form1(all_data)
        if form1_results:
            filename = f'form1_analysis_{timestamp}.csv'
            fieldnames = ['user_id', 'hardness_1_avg', 'hardness_2_avg', 'hardness_3_avg', 'hardness_4_avg', 'cot_quality_avg']
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(form1_results)
            print(f"✅ Exported: {filename}\n")
            
            # Display results
            print("\n📋 Form 1 Results:")
            print("-" * 100)
            print(f"{'User ID':<25} {'H1 Avg':<10} {'H2 Avg':<10} {'H3 Avg':<10} {'H4 Avg':<10} {'CoT Avg':<10}")
            print("-" * 100)
            for row in form1_results:
                h1 = f"{row['hardness_1_avg']:.2f}" if row['hardness_1_avg'] is not None else "N/A"
                h2 = f"{row['hardness_2_avg']:.2f}" if row['hardness_2_avg'] is not None else "N/A"
                h3 = f"{row['hardness_3_avg']:.2f}" if row['hardness_3_avg'] is not None else "N/A"
                h4 = f"{row['hardness_4_avg']:.2f}" if row['hardness_4_avg'] is not None else "N/A"
                cot = f"{row['cot_quality_avg']:.2f}" if row['cot_quality_avg'] is not None else "N/A"
                print(f"{row['user_id']:<25} {h1:<10} {h2:<10} {h3:<10} {h4:<10} {cot:<10}")
            print()
        
        # Form 2: Model scores
        print("📊 Analyzing Form 2 (Model Evaluation)...")
        form2_results = analyze_form2(all_data)
        if form2_results:
            filename = f'form2_analysis_{timestamp}.csv'
            fieldnames = ['user_id', 'huatuo_avg', 'm1_avg', 'medreason_avg', 
                         'qwen8b_zs_avg', 'qwen8b_nocot_avg', 'qwen8b_sft_avg', 'qwen8b_rl_avg']
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(form2_results)
            print(f"✅ Exported: {filename}\n")
            
            # Display results
            print("\n📋 Form 2 Results:")
            print("-" * 140)
            print(f"{'User ID':<25} {'Huatuo':<12} {'m1':<12} {'MedReason':<12} {'Qwen ZS':<12} {'Qwen NC':<12} {'Qwen SFT':<12} {'Qwen RL':<12}")
            print("-" * 140)
            for row in form2_results:
                huatuo = f"{row['huatuo_avg']:.2f}" if row['huatuo_avg'] is not None else "N/A"
                m1 = f"{row['m1_avg']:.2f}" if row['m1_avg'] is not None else "N/A"
                medreason = f"{row['medreason_avg']:.2f}" if row['medreason_avg'] is not None else "N/A"
                zs = f"{row['qwen8b_zs_avg']:.2f}" if row['qwen8b_zs_avg'] is not None else "N/A"
                nc = f"{row['qwen8b_nocot_avg']:.2f}" if row['qwen8b_nocot_avg'] is not None else "N/A"
                sft = f"{row['qwen8b_sft_avg']:.2f}" if row['qwen8b_sft_avg'] is not None else "N/A"
                rl = f"{row['qwen8b_rl_avg']:.2f}" if row['qwen8b_rl_avg'] is not None else "N/A"
                print(f"{row['user_id']:<25} {huatuo:<12} {m1:<12} {medreason:<12} {zs:<12} {nc:<12} {sft:<12} {rl:<12}")
            print()
        
        # Form 3: CoT quality
        print("📊 Analyzing Form 3 (CoT Quality Evaluation)...")
        form3_results = analyze_form3(all_data)
        if form3_results:
            filename = f'form3_analysis_{timestamp}.csv'
            fieldnames = ['user_id', 'cot_quality_avg']
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(form3_results)
            print(f"✅ Exported: {filename}\n")
            
            # Display results
            print("\n📋 Form 3 Results:")
            print("-" * 40)
            print(f"{'User ID':<25} {'CoT Quality Avg':<12}")
            print("-" * 40)
            for row in form3_results:
                cot = f"{row['cot_quality_avg']:.2f}" if row['cot_quality_avg'] is not None else "N/A"
                print(f"{row['user_id']:<25} {cot:<12}")
            print()
        
        print("\n" + "="*80)
        print("✅ Analysis complete!")
        print("="*80)
        print(f"\n📁 Generated files:")
        print(f"   - form1_analysis_{timestamp}.csv  (Hardness ratings: user vs system)")
        print(f"   - form2_analysis_{timestamp}.csv  (Model scores)")
        print(f"   - form3_analysis_{timestamp}.csv  (CoT quality)")
        print("\n💡 Each file includes per-user rows plus an 'ALL_USERS' summary row\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
